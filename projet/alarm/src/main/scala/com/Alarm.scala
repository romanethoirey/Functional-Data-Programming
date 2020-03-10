package com

import scala.io.Source
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata, Callback}

object Alarm {

  import java.util.Properties

  val TOPIC = "alarm"

  val  props = new Properties()

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")



  def main(args: Array[String]) : Unit = {

    if (args.length < 2) {
      println("Usage: ./csv_parser file server [server2 ...]")
    } else {
      val fileName = args.head
      if (new java.io.File(fileName).isFile) {
        val servers = args.drop(1).map(_ + ":9092").mkString(",")
        println(servers)
        props.put("bootstrap.servers", servers)

        val producer = new KafkaProducer[String, String](props)
        val source = Source.fromFile(fileName)
        source.getLines().foreach((line: String) =>
          producer.send(new ProducerRecord(TOPIC, "key", line))
        )
        source.close()
        producer.close()

      } else {
        println("File not found!")
      }

    }

  }

}
