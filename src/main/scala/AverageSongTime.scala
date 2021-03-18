import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.log4j.Logger
import org.apache.log4j.Level

object AverageSongTime {
  def main(args: Array[String]):Unit ={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Artist by average song length").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val data = sc.textFile("data.tsv").map(x=>(x.split("\t")(1), x.split("\t")(3))).
      mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it, preservesPartitioning = true).
      map({case(x,y) => (x.split(", ").toList, y)}).map({case(x,y) => x.map(z=>(z,y))}).flatMap(l=>l).
      map({case(x,y) => (x.replaceAll("""[\['\]?"]""", ""), y.toInt)}).groupByKey().
      map({case(x,y) => (x, y.aggregate((0,0))((x,y) => (x._1 + y, x._2 +1), (x,y) => (x._1 + y._1, x._2 + y._2)))}).
      map({case(x,y) => (x, (y._1 * 1.0 / y._2) / 1000)}).
      //sortBy(_._2).map({case(x,y) => s"$x: ${y}seconds"}). //shortest songwriters
      sortBy(-_._2).map({case(x,y) => s"$x: ${y}seconds or ${(y/60).toInt}minutes ${(y%60).toInt}seconds"}). //longest songwriters
      collect()
    println("") //So that output spacing is nice
    data.take(10).foreach(println(_))
  }
 """val spark = SparkSession.builder.appName("Simple Application").master("local[4]").getOrCreate()"""
 """val data_csv = spark.read.option("header", true)     .option("inferSchema", "true")      .csv("data/data.csv")"""
}
