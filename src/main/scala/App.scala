import org.apache.spark.SparkContext._

import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.apache.spark.util.LongAccumulator
import breeze.linalg._
import breeze.plot._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.evaluation.RegressionMetrics

import java.awt.Color
import scala.collection._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.io.{File, PrintWriter}

object App {

  // Citation: https://stackoverflow.com/questions/29908297/how-can-i-convert-a-json-string-to-a-scala-map
  def jsonToMap(json: String): immutable.HashMap[String, String] = {
    implicit val formats = org.json4s.DefaultFormats
    parse(json).extract[immutable.HashMap[String, String]]
  }

  def cleanText(text: String): String = {
    // TODO: contraction expansion
    // TODO: stemming/lemmatizing

    text
      .toLowerCase()
      .replaceAll("[\\t\\n\\r]+"," ")
      .replaceAll("[^a-zA-Z ]", "")
  }

  def splitRemoveStopWords(text: String, stopWords: immutable.HashSet[String]): Array[String] = {
    text
      .split("\\s+")
      .filter(x => !stopWords.contains(x))
  }

  // Takes normalized vectors x and y and computes the dot product
  // Dot product of two normalized vectors == cosine similarity of these vectors
  // From https://github.com/Xueping/spark-graph/blob/master/src/main/sources/org/apache/spark/mllib/linalg/BLAS.scala
  def cosSimilarityFromNorm(x: SparseVector, y: SparseVector): Double = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val yIndices = y.indices
    val nnzx = xIndices.length
    val nnzy = yIndices.length

    var kx = 0
    var ky = 0
    var sum = 0.0
    // y catching x
    while (kx < nnzx && ky < nnzy) {
      val ix = xIndices(kx)
      while (ky < nnzy && yIndices(ky) < ix) {
        ky += 1
      }
      if (ky < nnzy && yIndices(ky) == ix) {
        sum += xValues(kx) * yValues(ky)
        ky += 1
      }
      kx += 1
    }
    sum
  }

  def produceReviewsByRating(sc: SparkContext, reviews: RDD[(String, String)], numDocsPerRating: Broadcast[Int], writePath: String): Unit = {
    val reviewByRating = reviews
      .groupByKey()
      .map({case (rating, review_iter) => (rating, review_iter.take(numDocsPerRating.value).toList)})
      .flatMap({case (rating, review_list) => review_list.map(x => (rating, x))})
      .filter({case (rating, review) => rating.length > 0 && review.length > 0})
      .zipWithIndex()
      .map({case ((rating, review), row_num) => row_num + ", " + rating + ", " + cleanText(review)})
      .coalesce(1)
      .saveAsTextFile(writePath)
  }

  def produceTfIdfVectorsNormalized(sc: SparkContext, reviewsByRating: RDD[(String, String, String)],
                                    stopWords: Broadcast[immutable.HashSet[String]],
                                    numDocs: Broadcast[java.lang.Long],
                                    topNumWords: Broadcast[Int]): RDD[(String, SparseVector)] = {

    // Word counts for every document
    val wordCountsByDoc = reviewsByRating
      .map({ case (doc_id, rating, text) => (doc_id, rating, splitRemoveStopWords(text, stopWords.value)) })
      .map({ case (doc_id, rating, words) =>
        (doc_id,
          rating,
          words.length,
          immutable.HashMap(words.groupBy(w => w).mapValues(_.length).toSeq: _*))})
      .persist()

    // For every unique word in the corpus (across all documents), count how many documents that word appears in
    val wordDocumentFrequencyMap = sc.broadcast(wordCountsByDoc
      .map({ case (doc_id, rating, doc_total_words, doc_word_counts) => doc_word_counts.map(x => (x._1, 1)) })
      .fold(immutable.HashMap.empty[String, Int])({
        (map1, map2) => immutable.HashMap[String, Int] (
          (map1.toSeq ++ map2.toSeq).groupBy(_._1).mapValues(_.map(x => x._2).toList.sum).toSeq: _*
        )
      }))
    // TODO: Taking the top n most frequent words as these are most relevant to sentiment in this case. Nonfrequent
    // TODO: words are good for authorship attribution and business attribution.
    val wordDocumentFrequencyListOrdered = sc.broadcast(wordDocumentFrequencyMap.value.toSeq.sortBy(x => x._2 * -1).take(topNumWords.value))

    val norm = sc.broadcast(new Normalizer(2))
    val docTfIdfVectorsNormalized = wordCountsByDoc
      .map( {case (doc_id, rating, doc_total_words, doc_word_counts) =>
        (doc_id,
          doc_total_words,
          wordDocumentFrequencyListOrdered.value.map({ case (word, wordDocumentFrequency) => (word, doc_word_counts.getOrElse(word, 0))}))})
      .map({ case (doc_id, doc_total_words, corpus_words_in_doc_counts) =>
        (doc_id,
          corpus_words_in_doc_counts.map({case (word, doc_wc) =>
          (doc_wc.toDouble/doc_total_words.toDouble) * math.log(numDocs.value.toDouble/wordDocumentFrequencyMap.value(word).toDouble)}))})
      .map({ case (row_num, tfidf_vector) => (row_num, norm.value.transform(Vectors.dense(tfidf_vector.toArray)).toSparse)})

    wordCountsByDoc.unpersist()
    return docTfIdfVectorsNormalized
  }

  def produceSimilarityVectors(sc: SparkContext, docTfIdfVectorsNormalized: RDD[(String, SparseVector)], writePath: String): Unit = {
    val similarities = docTfIdfVectorsNormalized.cartesian(docTfIdfVectorsNormalized)
      .filter({ case ((id1, vector1), (id2, vector2)) => !id1.equals(id2) })
      .map({ case ((id1, vector1), (id2, vector2)) => (List(id1, id2).sorted, cosSimilarityFromNorm(vector1, vector2)) })
      .reduceByKey({ (x, y) => x })
      .map({ case (id_pair, similarity) => ((id_pair(0), id_pair(1)), similarity) })
//      .sortBy({ case ((id1, id2), sim) => sim * -1.0 }) // no need to sort these anymore
      .map({case ((id1, id2), sim) => id1 + ", " + id2 + ", " + sim})
      .coalesce(1)
      .saveAsTextFile(writePath)
  }

  def getKNN(sc: SparkContext,
             similarities: RDD[((String, String), Double)],
             ratingsActual: RDD[(String, Double)],
             k: Int): RDD[(String, List[Double])] = {
    val knn = similarities
      .flatMap({case ((id1, id2), sim) => List((id1, (id2, sim)), (id2, (id1, sim)))})
      .map({case (main_id, (compare_id, sim)) => (compare_id, (main_id, sim))})
      .join(ratingsActual)
      .map({case (compare_id, ((main_id, sim), rating)) => (main_id, (compare_id, sim, rating))})
      .groupByKey()
      // enforce secondary sort on id to maintain result consistency across runs?
      .mapValues(x => x.toList.sortBy(el => (el._2 * -1, el._1)).map(x => x._3).take(k))

    return knn
  }

  def calcClassificationMetrics(sc: SparkContext,
                                actualPredicted: RDD[(String, Double, Double)],
                                numDocsPerRating: Int,
                                numWordsInVector: Int): Unit = {
    // https://spark.apache.org/docs/2.2.0/mllib-evaluation-metrics.html#regression-model-evaluation
    val metricsRDD = actualPredicted.map({ case (doc_id, pred, actual) => (pred, actual) })
    val metrics = new MulticlassMetrics(metricsRDD)
    val pw = new PrintWriter(new File(s"output/classification_docs${numDocsPerRating}_words${numWordsInVector}"))

    val labels = metrics.labels

    pw.write("Confusion Matrix\n")
    pw.write(metrics.confusionMatrix.toString() + "\n")
    pw.write("---\n")

    def makeHistogramData(confusionMatrixFlat: Array[Double], ranking: Int): Array[Double] = {
      var actualRank = ranking
      if (ranking == 5) {
        actualRank = 0
      }
      return confusionMatrixFlat
        .zipWithIndex
        .filter({case (e, i) => (i+1) % 5 == actualRank})
        .zipWithIndex
        .flatMap({case ((e, old_i), new_i) => List.fill(e.toInt)((new_i+1).toDouble)})
    }

    val confusionMatrixFlat = metrics.confusionMatrix.toArray

    val f = Figure()
    val plot1 = f.subplot(3, 2, 0)
    plot1 += hist(makeHistogramData(confusionMatrixFlat, 1), 5)
    plot1.title = "Prediction Distribution for Actual Rating of 1.0"
    plot1.xlabel = "Prediction Rating"
    plot1.ylabel = "Number of Predictions"

    val plot2 = f.subplot(3, 2, 1)
    plot2 += hist(makeHistogramData(confusionMatrixFlat, 2), 5)
    plot2.title = "Prediction Distribution for Actual Rating of 2.0"
    plot2.xlabel = "Prediction Rating"
    plot2.ylabel = "Number of Predictions"

    val plot3 = f.subplot(3, 2, 2)
    plot3 += hist(makeHistogramData(confusionMatrixFlat, 3), 5)
    plot3.title = "Prediction Distribution for Actual Rating of 3.0"
    plot3.xlabel = "Prediction Rating"
    plot3.ylabel = "Number of Predictions"

    val plot4 = f.subplot(3, 2, 3)
    plot4 += hist(makeHistogramData(confusionMatrixFlat, 4), 5)
    plot4.title = "Prediction Distribution for Actual Rating of 4.0"
    plot4.xlabel = "Prediction Rating"
    plot4.ylabel = "Number of Predictions"

    val plot5 = f.subplot(3, 2, 4)
    plot5 += hist(makeHistogramData(confusionMatrixFlat, 5), 5)
    plot5.title = "Prediction Distribution for Actual Rating of 5.0"
    plot5.xlabel = "Prediction Rating"
    plot5.ylabel = "Number of Predictions"
    f.saveas(s"output/classification_histos_docs${numDocsPerRating}_words${numWordsInVector}.png", 300)

    pw.write(s"Overall Accuracy: ${metrics.accuracy}\n")
    pw.write("---\n")

    pw.write("Precision By Label\n")
    labels.foreach { l =>
      pw.write(s"Precision($l) = " + metrics.precision(l) + "\n")
    }
    pw.write("---\n")

    pw.write("Recall By Label\n")
    labels.foreach { l =>
      pw.write(s"Recall($l) = " + metrics.recall(l) + "\n")
    }
    pw.write("---\n")

    pw.write("F-Measure By Label\n")
    // F-measure by label
    labels.foreach { l =>
      pw.write(s"F1-Score($l) = " + metrics.fMeasure(l) + "\n")
    }
    pw.write("---\n")
    pw.close()
  }

  def calcRegressionMetrics(sc: SparkContext,
                            actualPredicted: RDD[(String, Double, Double)],
                            numDocsPerRating: Int,
                            numWordsInVector: Int): Unit = {
    //https://spark.apache.org/docs/2.2.0/mllib-evaluation-metrics.html#multiclass-classification
    val metricsRDD = actualPredicted.map({ case (doc_id, pred, actual) => (pred, actual) })
    val metrics = new RegressionMetrics(metricsRDD)
    val pw = new PrintWriter(new File(s"output/regression_docs${numDocsPerRating}_words${numWordsInVector}"))

    val metricsSeq = metricsRDD.collect()

    val f = Figure()
    val plot1 = f.subplot(3, 2, 0)
    plot1 += hist(metricsSeq.filter(x => x._2 == 1.0).map(x => x._1), 50)
    plot1.title = "Prediction Distribution for Actual Rating of 1.0"
    plot1.xlabel = "Prediction Rating"
    plot1.ylabel = "Number of Predictions"

    val plot2 = f.subplot(3, 2, 1)
    plot2 += hist(metricsSeq.filter(x => x._2 == 2.0).map(x => x._1), 50)
    plot2.title = "Prediction Distribution for Actual Rating of 2.0"
    plot2.xlabel = "Prediction Rating"
    plot2.ylabel = "Number of Predictions"

    val plot3 = f.subplot(3, 2, 2)
    plot3 += hist(metricsSeq.filter(x => x._2 == 3.0).map(x => x._1), 50)
    plot3.title = "Prediction Distribution for Actual Rating of 3.0"
    plot3.xlabel = "Prediction Rating"
    plot3.ylabel = "Number of Predictions"

    val plot4 = f.subplot(3, 2, 3)
    plot4 += hist(metricsSeq.filter(x => x._2 == 4.0).map(x => x._1), 50)
    plot4.title = "Prediction Distribution for Actual Rating of 4.0"
    plot4.xlabel = "Prediction Rating"
    plot4.ylabel = "Number of Predictions"

    val plot5 = f.subplot(3, 2, 4)
    plot5 += hist(metricsSeq.filter(x => x._2 == 5.0).map(x => x._1), 50)
    plot5.title = "Prediction Distribution for Actual Rating of 5.0"
    plot5.xlabel = "Prediction Rating"
    plot5.ylabel = "Number of Predictions"
    f.saveas(s"output/regression_histos_docs${numDocsPerRating}_words${numWordsInVector}.png", 300)

    // Squared error
    pw.write(s"MSE = ${metrics.meanSquaredError}\n")
    pw.write(s"RMSE = ${metrics.rootMeanSquaredError}\n")

    // R-squared
    pw.write(s"R-Squared = ${metrics.r2}\n")

    // Mean absolute error
    pw.write(s"MAE = ${metrics.meanAbsoluteError}\n")

    // Explained variance
    pw.write(s"Explained Variance = ${metrics.explainedVariance}\n")
    pw.close()
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("App").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Determines type of analysis to run
    // "false": runs KNN categorical classification on the 5 rating possibilities
    //      - A review rating prediction will be the plurality class of the K nearest neighbors and will be categorical
    //        (i.e. either 1, 2, 3, 4, or 5
    // "true": runs KNN regression on the 5 rating possibilities
    //      - A review rating prediction will be continuous and the average of the K nearest neighbors
    val run_regression = false

    // INPUT PATHS
    val yelpReviewsInputPath = "input/reviews/yelp_academic_dataset_review.json"
    val stopWordsInputPath = "input/stopWords/stopWords.txt"
    // OUTPUT PATHS
    val reviewsByRatingOutputPath = "input/reviewsByRating"
    val similaritiesOutputPath = "input/similarities"
    // PARAMETERS
    val numDocsPerRating = sc.broadcast(500) // Number of documents per rating to consider (equivalent for balanced dataset)
    val numWordsInVector = sc.broadcast(1000) // Number of words to consider throughout corpus for a TF-IDF vector
    val stopWords = sc.broadcast(immutable.HashSet() ++ sc.textFile(stopWordsInputPath) // Words to not consider for calculating TF-IDF
      .map(x => (x, ""))
      .collectAsMap()
      .keys)

    // STEP 1: Get numDocsPerRating number of reviews for each rating = (numDocsPerRating * 5) [5 ratings]
    val reviews = sc.textFile(yelpReviewsInputPath)
      .map(x => jsonToMap(x)) // Convert JSON to map for better interaction
      .map(x => (x("stars"), x("text")))

    // Writes to file
    produceReviewsByRating(sc, reviews, numDocsPerRating, reviewsByRatingOutputPath)

    // : STEP 2: Create normalized TFIDF vectors, calculate similarities and write similarities to file
    val numDocsAcc = sc.longAccumulator("numDocs")
    val reviewsByRating = sc.textFile(reviewsByRatingOutputPath + "/part-00000")
      .map(x => x.split(", ", 3))
      .map(x => (x(0), x(1), x(2)))
      .persist()

    reviewsByRating.foreach(x => numDocsAcc.add(1)) // Calculate number of documents in corpus
    val numDocs = sc.broadcast(numDocsAcc.value) // Cast numDocs as an immutable broadcast variable
    println(s"Number of Documents: ${numDocs.value}")

    // Not writing doc vectors to file as they are very large and this will increase computational demand
    val docTfIdfVectorsNormalized = produceTfIdfVectorsNormalized(sc, reviewsByRating, stopWords, numDocs, numWordsInVector)
    docTfIdfVectorsNormalized.persist()

    // No longer need the text of the document, use ratings actual from now on
    val ratingsActual = reviewsByRating
      .map({ case (id, rating, text) => (id, rating.toDouble) })
      .persist()
    reviewsByRating.unpersist()

    // Writes to file
    produceSimilarityVectors(sc, docTfIdfVectorsNormalized, similaritiesOutputPath)
    docTfIdfVectorsNormalized.unpersist()

    // STEP 4: Calculate KNN Predictions
    val similarities = sc.textFile(similaritiesOutputPath + "/part-00000")
      .map(x => x.split(", "))
      .map(x => ((x(0), x(1)), x(2).toDouble))

    val knnByDoc = getKNN(sc, similarities, ratingsActual, math.sqrt(numDocs.value.toDouble).toInt)

    if (run_regression) {
      // Do regression analysis based on average of k nearest neighbors
      val predictions = knnByDoc.mapValues(x => x.sum/x.length) // get average of classes of knn

      val actualPredicted = predictions
        .join(ratingsActual)
        .map({ case (doc_id, (pred, actual)) => (doc_id, pred, actual) })

      ratingsActual.unpersist()

      calcRegressionMetrics(sc, actualPredicted, numDocsPerRating.value, numWordsInVector.value)
    }
    else {
      // Do categorical classification analysis based on plurality class of k nearest neighbors
      val predictions = knnByDoc.mapValues(x => x.groupBy(identity).maxBy(_._2.size)._1) // get plurality class of knn

      val actualPredicted = predictions
        .join(ratingsActual)
        .map({ case (doc_id, (pred, actual)) => (doc_id, pred, actual) })

      ratingsActual.unpersist()

      calcClassificationMetrics(sc, actualPredicted, numDocsPerRating.value, numWordsInVector.value)
    }
  }
}
