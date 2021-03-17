import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


object TopThreePerGenre {
    def main(args: Array[String]):Unit = {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)

      val conf = new SparkConf().setAppName("CSC369Project").setMaster("local[4]")
      val sc = new SparkContext(conf)



    }

}
