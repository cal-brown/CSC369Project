import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


object danceabilityOverTime {

  def main(args: Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    System.setProperty("hadoop.home.dir", "c:/winutils/")

    val conf = new SparkConf().setAppName("sg1").setMaster("local[4]")
    val sc = new SparkContext(conf)

    println()
    val data = sc.textFile("data/data.tsv").map(x => (x.split("\t")(18), x.split("\t")(2))).
      mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it, preservesPartitioning = true).
      map(x => (x._1, x._2.toDouble)).
      mapValues( v=> (v,1)).
      reduceByKey( (x,y) => (x._1+y._1,x._2+y._2)).
      mapValues{ case(x,y) => x*1.0/y}.
      sortBy(x => x._2 * -1).take(5).foreach(println)









  }

}
