import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


object EnergyAffectPopularity {
  def main(args: Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val POPULAR_THRESHOLD = 50

    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rowRdd = sc.textFile("data/data.tsv")
    val data = rowRdd.mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it,
      preservesPartitioning = true)
      .map(line => (line.split("\t")(18).trim().toInt, (line.split("\t")(4).trim().toDouble, line.split("\t")(13).trim().toInt)));



    val groupedByYear = data.filter(_._2._2 > POPULAR_THRESHOLD)
      .map({case(year, (energy, popularity)) => (year, (((popularity - energy * 100) / popularity)))})
      .groupByKey()
      .map({case(year, energyRatio) => (year, energyRatio.sum / energyRatio.size)})

    groupedByYear.foreach(println(_))


    groupedByYear.sortByKey().take(10000).foreach(x => println("In "  + x._1 + (if(-0.1 < x._2 && x._2 < 0.1) " A song's popularity and energy showed a correlation" else " A song's popularity and energy did not show a correlation")))  }

}
