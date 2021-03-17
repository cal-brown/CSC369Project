import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.log4j.Logger
import org.apache.log4j.Level

object MostPopularArtists {
  def main(args: Array[String]): Unit = {
    //allYears() //All years
    lastHalfCentury() //last 50 years
  }

  def allYears():Unit ={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Artist with most popular songs").setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.textFile("data.csv").map(x=>(x.split(",")(1), x.split(",")(13))).
      mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it,
        preservesPartitioning = true).filter(_._2 != "0").
      map({case(x,y) => (x.replaceAll("""[\['\]?"]""", ""), y)}).
      countByKey().toList.sortBy(-_._2).take(10).foreach(println(_))
  }

  def lastHalfCentury(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Artist with most popular songs").setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.textFile("data.csv").map(x=>(x.split(",")(1), x.split(",")(13), x.split(",")(14))).
      mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it,
        preservesPartitioning = true).filter(_._2 != "0").filter(_._3.matches("[0-9]+")).
      map({case(x,y,z) => (x.replaceAll("""[\['\]?"]""", ""),( y, z.toInt))}).filter(_._2._2 > 1970).
      countByKey().toList.sortBy(-_._2).take(10).foreach(println(_))
  }
}
