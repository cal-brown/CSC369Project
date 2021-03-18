import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


object ExplicitPopularity {
  def main(args: Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val POPULAR_THRESHOLD = 50

    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rowRdd = sc.textFile("data/data.tsv")
    val data = rowRdd.mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it,
      preservesPartitioning = true)
      .map(line => (line.split("\t")(18).trim().toInt, (line.split("\t")(5).trim().toInt, line.split("\t")(13).trim().toInt)));

    val explicitGroupedByYear = data.filter(_._2._2 > POPULAR_THRESHOLD).filter(_._2._1 == 1)
      .map({case(year, (explicitTag, popularity)) => (year, popularity)}).groupByKey()
      .map({case (year, popularity) => (year, popularity.size)})
    val nonExplicitGroupedByYear = data.filter(_._2._2 > POPULAR_THRESHOLD).filter(_._2._1 == 0)
      .map({case(year, (explicitTag, popularity)) => (year, popularity)}).groupByKey()
      .map({case (year, popularity) => (year, popularity.size)})


    val popPercentages = explicitGroupedByYear.join(nonExplicitGroupedByYear)
      .map({case (year, (exp, nonExp)) => (year, ((exp.toFloat / (exp + nonExp).toFloat) * 100, (nonExp.toFloat / (exp + nonExp).toFloat) * 100 ))})

    popPercentages.sortByKey().take(1000).foreach(x => println("Year: " + x._1 + ", Explicit: " + "%.0f".format(x._2._1) + "%, Non-Explicit: " + "%.0f".format(x._2._2) + "%"))
  }

}