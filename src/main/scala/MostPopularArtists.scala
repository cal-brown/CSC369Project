import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.log4j.Logger
import org.apache.log4j.Level

object MostPopularArtists {
  def main(args: Array[String]): Unit = {
    allYears() //All years
    //lastHalfCentury() //last 50 years
  }

  def allYears():Unit ={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Artist with most popular songs").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val data = sc.textFile("data.tsv").map(x=>(x.split("\t")(1), x.split("\t")(13))).
      mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it, preservesPartitioning = true).
      map({case(x,y) => (x.split(", ").toList, y)}).map({case(x,y) => x.map(z=>(z,y))}).flatMap(l=>l).
      filter(_._2 != "0").map({case(x,y) => (x.replaceAll("""[\['\]?"]""", ""), y)}).
      countByKey().toList.sortBy(-_._2)
    println("") //So that output spacing is nice
    data.take(10).foreach(println(_))
  }

  def lastHalfCentury(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Artist with most popular songs").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val data = sc.textFile("data.tsv").map(x=>(x.split("\t")(1), x.split("\t")(13), x.split("\t")(14))).
      mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it, preservesPartitioning = true).
      map({case(x,y,z) => (x.split(", ").toList, y, z)}).map({case(x,y,z) => x.map(q=>(q,y,z))}).flatMap(l=>l).
      filter(_._2 != "0").map({case(x,y,z) => (x.replaceAll("""[\['\]?"]""", ""),(y, z.split("-")(0).toInt))}).
      filter(_._2._2 > 1970).countByKey().toList.sortBy(-_._2)
    println("") //So that output spacing is nice
    data.take(10).foreach(println(_))
  }
}
