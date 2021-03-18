import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LassoWithSGD, LinearRegressionWithSGD, RidgeRegressionWithSGD}
import org.apache.spark.mllib.evaluation.RegressionMetrics

object PopularityPredictor {
  def main(args: Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rowRdd = sc.textFile("data/data.tsv")
      .map(_.split("\t"))
    val restData = rowRdd.mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it,
      preservesPartitioning = true)

    val dataSets = restData.randomSplit(Array(0.3, 0.7), 1)
    val testing = dataSets(0)
    val training = dataSets(1)
    val train_acousticness = training.map(_(0).toDouble)
    val train_danceability = training.map(_(2).toDouble)
    val train_duration_ms = training.map(_(3).toDouble)
    val train_energy = training.map(_(4).toDouble)
    val train_instrumentalness = training.map(_(7).toDouble)
    val train_liveness = training.map(_(9).toDouble)
    val train_loudness = training.map(_(10).toDouble)
    val train_popularity = training.map(_(13).toDouble)
    val train_speechiness = training.map(_(15).toDouble)
    val train_tempo = training.map(_(16).toDouble)
    val train_valence = training.map(_(17).toDouble)

    val test_acousticness = testing.map(_(0).toDouble)
    val test_danceability = testing.map(_(2).toDouble)
    val test_duration_ms = testing.map(_(3).toDouble)
    val test_energy = testing.map(_(4).toDouble)
    val test_instrumentalness = testing.map(_(7).toDouble)
    val test_liveness = testing.map(_(9).toDouble)
    val test_loudness = testing.map(_(10).toDouble)
    val test_popularity = testing.map(_(13).toDouble)
    val test_speechiness = testing.map(_(15).toDouble)
    val test_tempo = testing.map(_(16).toDouble)
    val test_valence = testing.map(_(17).toDouble)

    val factors = List((train_acousticness, test_acousticness), (train_danceability, test_danceability), (train_duration_ms, test_duration_ms),
      (train_energy, test_energy), (train_instrumentalness, test_instrumentalness),
      (train_liveness, test_liveness), (train_loudness, test_loudness),
      (train_speechiness, test_speechiness), (train_tempo, test_tempo), (train_valence, test_valence));
    val correlations = factors.map(f => (f, Math.abs(Statistics.corr(f._1, train_popularity))))

    val relF = correlations.sortWith((x, y) => x._2 > y._2).take(5)
    val trainingFactors = relF.map(x => x._1._1)
    val testingFactors = relF.map(x => x._1._2)
    val trainingFinal = trainingFactors(0).zip(trainingFactors(1)).zip(trainingFactors(2)).zip(trainingFactors(3)).zip(trainingFactors(4))
      .map({case ((((a, b), c), d), e) => (a, b, c, d, e)})
      .zip(train_popularity.map(_-1).map(_.asInstanceOf[Int]))
      .map({case ((a, b, c, d, e), pop) => LabeledPoint(pop, Vectors.dense(a, b, c, d, e))})
    val testingFinal = testingFactors(0).zip(testingFactors(1)).zip(testingFactors(2)).zip(testingFactors(3)).zip(testingFactors(4))
      .map({case ((((a, b), c), d), e) => (a, b, c, d, e)})
      .zip(test_popularity.map(_-1).map(_.asInstanceOf[Int]))
      .map({case ((a, b, c, d, e), pop) => LabeledPoint(pop, Vectors.dense(a, b, c, d, e))})

    val numIterations = 1000
    val LinRegModel = LinearRegressionWithSGD.train(trainingFinal, numIterations)
    val LassoModel = LassoWithSGD.train(trainingFinal, numIterations)
    val RidgeRegModel = RidgeRegressionWithSGD.train(trainingFinal, numIterations)

    val LinRegTrainResults = trainingFinal.map { point =>
      val prediction = LinRegModel.predict(point.features)
      (point.label, prediction)
    }
    val LinRegTestResults = testingFinal.map { point =>
      val prediction = LinRegModel.predict(point.features)
      (point.label, prediction)
    }
    val RidgeTrainResults = trainingFinal.map { point =>
      val prediction = RidgeRegModel.predict(point.features)
      (point.label, prediction)
    }
    val RidgeTestResults = testingFinal.map { point =>
      val prediction = RidgeRegModel.predict(point.features)
      (point.label, prediction)
    }
    val LassoTrainResults = trainingFinal.map { point =>
      val prediction = LassoModel.predict(point.features)
      (point.label, prediction)
    }
    val LassoTestResults = testingFinal.map { point =>
      val prediction = LassoModel.predict(point.features)
      (point.label, prediction)
    }
    val LinRegTrainMetrics = new RegressionMetrics(LinRegTrainResults)
    val LinRegTestMetrics = new RegressionMetrics(LinRegTestResults)
    val LassoTrainMetrics = new RegressionMetrics(LassoTrainResults)
    val LassoTestMetrics = new RegressionMetrics(LassoTestResults)
    val RidgeRegTrainMetrics = new RegressionMetrics(RidgeTrainResults)
    val RidgeRegTestMetrics = new RegressionMetrics(RidgeTestResults)

    val metrics = List(LinRegTestMetrics, LinRegTrainMetrics, LassoTrainMetrics, LassoTestMetrics, RidgeRegTrainMetrics,
      RidgeRegTestMetrics)
    val metricNames = List("LinRegTestMetrics", "LinRegTrainMetrics", "LassoTrainMetrics", "LassoTestMetrics", "RidgeRegTrainMetrics",
      "RidgeRegTestMetrics")
    metrics.zip(metricNames).foreach(m => println(s"${m._2} R-squared=${m._1.r2} MSE = ${m._1.meanSquaredError}"))
  }
}