import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}


object PopularityPredictor {
  def main(args: Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rowRdd = sc.textFile("data/data.csv")
      .map(_.split(","))
    val header = Row.fromSeq(rowRdd.first)
    rowRdd.take(2).foreach(x => println(x.mkString(",")))
    val data = rowRdd.mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it,
      preservesPartitioning = true)
      .map(p => List(p(0).toDouble, getArtistList(p(1)), p(2).toDouble, p(3).toInt, p(4).toDouble, p(5) == 1,
        p(6), p(7).toDouble, p(8).toInt, p(9).toDouble, p(10).toDouble, p(11)==1,
        p(12), p(13).toInt, p(14), p(15).toDouble, p(16).toDouble, p(17).toDouble, p(18)))
      .map(p => Row.fromSeq(p))

  }

  def getArtistList(lst: String): Array[String] = {
    val trimmed_lst = lst.substring(1, lst.length() - 1)
    trimmed_lst.split(",").map(name => name.substring(0, name.length()-1))
  }
}
