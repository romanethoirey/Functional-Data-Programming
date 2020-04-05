import java.util.{Collections, Properties}
import java.util.regex.Pattern
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._


object Alarm {

  def main(args: Array[String]) : Unit = {
    println("Program Started")


    val props:Properties = new Properties()
    props.put("group.id", "alarm")
    props.put("bootstrap.servers","localhost:9092")
    props.put("zookeeper.connect", "localhost:2181")
    props.put("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    val consumer = new KafkaConsumer(props)
    val topics = List("DRONE")
    try {
      consumer.subscribe(topics.asJava)
      while (true) {
        val records = consumer.poll(10)

        // We use a forEach loop to look for each record
        for (record <- records.iterator().asScala) {

          val values = record.value().asInstanceOf[String];
          val codeError = values.split(",")(5);

          if (codeError.equals("99")) {
            println("Need human intervention - Beep beep");
            println(values);
          }
        }
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      consumer.close()
    }
  }

}