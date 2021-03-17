import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.log4j.Logger
import org.apache.log4j.Level

object ArtistBySongs {
  def main(args: Array[String]) {
    //allYears() //All Years
    lastHalfCentury() //Last 50 years

  }
  def allYears(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Artist with most songs").setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.textFile("data.csv").map(_.split(",")(1)).mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it,
      preservesPartitioning = true).map(_.replaceAll("""[\['\]?"]""", "")).countByValue().toList.
      sortBy(-_._2).take(10).foreach(println(_))
  }

  def lastHalfCentury(): Unit ={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Artist with most songs").setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.textFile("data.csv").map(x=>(x.split(",")(1), x.split(",")(14))).
      mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it,
      preservesPartitioning = true).map({case(x,y) => (x.replaceAll("""[\['\]?"]""", ""), y.split("-")(0))}).
      filter(_._2.matches("[0-9]+")).map({case(x,y) => (x,y.toInt)}).
      filter(_._2 > 1970).countByKey().toList.
      sortBy(-_._2).take(10).
      foreach(println(_))
  }

}

