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
    val data = sc.textFile("data.tsv").map(_.split("\t")(1)).mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it,
      preservesPartitioning = true).map(x=>x.split(", ").toList.map(x=>(x,1))).flatMap(l=>l).
      map(_._1.replaceAll("""[\['\]?"]""", "")).countByValue().toList.sortBy(-_._2).map({case(x,y)=>s"$x released $y songs"})
    println("") //So that output spacing is nice
    data.take(10).foreach(println(_))
  }

  def lastHalfCentury(): Unit ={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Artist with most songs").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val data = sc.textFile("data.tsv").map(x=>(x.split("\t")(1), x.split("\t")(14))).
      mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it, preservesPartitioning = true).
      map({case(x,y)=>x.split(",").map(z=>(z,y))}).flatMap(l=>l).
      map({case(x,y) => (x.replaceAll("""[\['\]?"]""", ""), y.split("-")(0).toInt)}).
      filter(_._2 > 1970).countByKey().toList.sortBy(-_._2).map({case(x,y)=>s"${x.trim()} released $y songs"})
    println("") //So that output spacing is nice
    data.take(10).foreach(println(_))
  }

}

