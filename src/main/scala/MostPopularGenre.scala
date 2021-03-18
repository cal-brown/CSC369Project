import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MostPopularGenre {
  def main(args: Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    System.setProperty("hadoop.home.dir", "c:/winutils/")

    val conf = new SparkConf().setAppName("sg1").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //(genre, popularity rating)
    val data = sc.textFile("data/data_by_genres.csv").map(x => (x.split(",")(0), x.split(",")(11))).
      mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it, preservesPartitioning = true).
      map(x => (x._1, x._2.toDouble))

    //sort in ascending
    data.sortBy(_._2 * -1).
      take(5).
      foreach(println)
  }
}
