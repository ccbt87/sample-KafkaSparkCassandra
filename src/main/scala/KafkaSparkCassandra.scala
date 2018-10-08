import com.datastax.spark.connector._
import com.datastax.driver.core.Cluster
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

object KafkaSparkCassandra {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .master("yarn") // change to correct master
      .appName("Test")
      .config("spark.executor.memory", "1g")
      .config("spark.cassandra.connection.host", "hostname") // change to correct cassandra hostname
      .config("spark.cassandra.auth.username", "")
      .config("spark.cassandra.auth.password", "")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", ".")
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext
    // Create spark streaming context with 5 second batch interval
    val ssc = new StreamingContext(sc, Seconds(5))

    // Set the logging level to reduce log message spam
    ssc.sparkContext.setLogLevel("ERROR")

    // create a timer that we will use to stop the processing after 60 seconds so we can print some results
    val timer = new Thread() {
      override def run() {
        Thread.sleep(1000 * 60)
        ssc.stop(stopSparkContext=false, stopGracefully=true) 
      }
    }

    // Kafka
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "sandbox-hdp.hortonworks.com:6667", // change to correct kafka bootstrap server
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "spark-streaming"
    )
    val topicsSet = Array[String]("test")
    val stream = KafkaUtils.createDirectStream[String,String](ssc,PreferConsistent,Subscribe[String,String](topicsSet,kafkaParams))

    // a single kafka message is like the following:
    // {"temperature":"87.31","eventTime":"1538776806360","deviceId":"0000000056789679"}
    
    val inputSchema = StructType(List(StructField("deviceId", StringType, true), 
      StructField("eventTime", StringType, true), 
      StructField("temperature", StringType, true)))

    val messages = stream.map(_.value)
    messages.print()
    messages.transform { rdd =>
      if(rdd.isEmpty) {
        spark.createDataFrame(sc.emptyRDD[Row], inputSchema).rdd
      }
      else {
        spark.read.schema(inputSchema).json(rdd.toDS())
          .filter($"eventTime".isNotNull && length(trim($"eventTime")) > 0)
          .filter($"temperature".isNotNull && length(trim($"temperature")) > 0)
          .filter($"deviceId".isNotNull && length(trim($"deviceId")) > 0)
          .withColumn("eventTime", to_timestamp($"eventTime"/1000))
          .withColumn("temperature", ($"temperature").cast("int"))
          .dropDuplicates("temperature")
          .rdd
      }
    }.foreachRDD { rdd =>      
      if(!rdd.isEmpty) {
        rdd.filter(line => !line.anyNull).saveToCassandra("temp","history", SomeColumns("deviceid", "eventtime", "temperature"))
      }
    }

    // Now we have set up the processing logic it's time to do some processing
    ssc.start() // start the streaming context
    //timer.start()
    ssc.awaitTermination() // block while the context is running (until it's stopped by the timer)
  }
}
