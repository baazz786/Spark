import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SaveMode
import org.apache.spark.SparkContext
import java.util.logging.Level
import java.util.logging.Logger

object KafkaebayProcessing {
   Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

   val (zkQuorum, group, topics, numThreads) = ("localhost:2181","kelly","sfdp","2")
  val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[4]")
  println("connected")
        
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(2))
  
  case class sfdp(IncidntNum:String,
      Category:String,	
      Descript:String,
      DayOfWeek:String,
      Date1:String,
      Time:String,	
      PdDistrict:String,
      Resolution:String,
      Address:String,
      X:String,
      Y:String,
      Location:String,
      PdId:String)
  def main(args: Array[String]): Unit = {

       val hiveContext = new HiveContext(sc)
       import hiveContext.implicits._
       hiveContext.setConf("hive.metastore.uris","thrift://localhost:9083")
       hiveContext.sql("use default")
       
        ssc.checkpoint("checkpoint")
        println("topic connected")
        
        
        val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
        val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
        
        lines.foreachRDD{rdd => 
          
             //val header = (rdd.map(x => x.split("\r\n")).first())(0)
             
             //val rows = sc.parallelize((rdd.map(x => x.split("\r\n")).first().filter(row => row!=header )))
             
             
          val rows = sc.parallelize((rdd.map(x => x.split("\n")).first()))
             
             println(rows.count+" Number of rows fetched from kafka topic")
              
    
             

             
             val df = rows.map(line => line.split(",")).map(c => sfdp(c(0),c(1),c(2),c(3),c(4),c(5),c(6),c(7),c(8),c(9),c(10),c(11),c(12)))            
                     
             val table = df.toDF
          val coun= table.select("Category").distinct.count
             
          println(coun)  
          
          table.show
        
              //table.write.mode(SaveMode.Append).saveAsTable("sfdp")
       }
  
        ssc.start
        ssc.awaitTermination()
   }
  

  
}
