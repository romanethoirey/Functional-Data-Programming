package com

import scala.io.Source
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerRecord, ConsumerConfig}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._

import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010._

object Alarm {

  import java.util.Properties
  import java.util


  def main(args: Array[String]) : Unit = {

    val sparkConf = new SparkConf()
      .setAppName("Alarm")
      .setMaster("spark://localhost:9092")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val TOPIC = Array("test")

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

    val consumerStrategy = Subscribe[String,String](TOPIC, kafkaParams)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      consumerStrategy)

    val lines = stream.map(_.value())
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

    // spark-submit --packages org.apache.spark:spark-streaming -kafka_2.10:1.6.0 --class "KafkaWordCount" --master local[4] target/scala-2.10/spark -kafka-project_2.10-1.0.jar localhost:2181 <group name> <topic name> <number of threads>


    //./bin/spark-submit \
    //  --class <main-class> \
    //  --master <master-url> \
    //  --deploy-mode <deploy-mode> \
    //  --conf <key>=<value> \
    //  ... # other options
    //  <application-jar> \
    //  [application-arguments]
  }

}
