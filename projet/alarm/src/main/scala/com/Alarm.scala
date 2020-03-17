import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object Alarm extends App {

  println("Program Started")

  val conf = new SparkConf().setMaster("local[2]").setAppName("Alarm")

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "spark-playground-group",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )
  val ssc = new StreamingContext(conf, Seconds(10))


  val inputStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](Array("drone"), kafkaParams))
  val processedStream = inputStream
    .flatMap(record => record.value.split(","))

  processedStream.print()
  ssc.start()
  ssc.awaitTermination()

}