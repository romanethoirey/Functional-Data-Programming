/*
 * How to run this code:
 * This file should live in projet projet/aws_example/src/main/scala/com,
 * and is run from the terminal in projet/aws_example.
 * In that folder's build.sbt file, we need a dependency for AWS client:
 * libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.3.32" 
 *
 * Compile with sbt compile, then
 * sbt "run AWS_ACCESS_KEY AWS_SECRET_KEY"
 * where the keys are given as strings. You could create a shell script
 * with these strings locally, but *the keys must not be added to git*
 */

import scala.collection.mutable.ListBuffer
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonServiceException
import com.amazonaws.services.s3.model.PutObjectRequest
import java.io.File
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.InputStreamReader
import java.io.FileOutputStream
import java.io.FileWriter
import org.apache.commons.io.IOUtils 
import java.util.{Collections, Properties}
import java.util.regex.Pattern
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._


/**
 * Example Scala code that uses an AWS client to do two things:
 * - Upload data by using an input stream
 * - Read data from an AWS file
 */
object KafkaToAws {
  val BUCKET_NAME = "fdpprojectcreatingnewbucket"
  val props:Properties = new Properties()

  def setKafkaProps(servers: String) : Unit = {
    props.put("group.id", "test")
    props.put("bootstrap.servers", servers);
    props.put("zookeeper.connect", "localhost:2181")
    props.put("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
  }


  def upload(client: AmazonS3Client, content:String) : Unit = {

    // create a new bucket
    client.createBucket(BUCKET_NAME)
    val scratchFile = File.createTempFile("prefix", "suffix");
    try {
      val bw = new BufferedWriter(new FileWriter(scratchFile))
      bw.write(content)
      bw.close()
      val r = scala.util.Random
      val fileName = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH_mm").format(LocalDateTime.now) + "_seed_" + r.nextInt(100000)
      val putObjectRequest = new PutObjectRequest(BUCKET_NAME, fileName, scratchFile);
      val putObjectResult = client.putObject(putObjectRequest);
    
    } finally {
      if(scratchFile.exists()) {
        scratchFile.delete();
      }
    }

  }


  def main(args: Array[String]) : Unit = {

    println(args.length)
    if (args.length < 3) {
      println("Usage: ./AwsExample AWS_ACCESS_KEY AWS_SECRET_KEY server [server2 ...]")
    } else {
      val AWS_ACCESS_KEY = args(0)
      val AWS_SECRET_KEY =  args(1)
      val servers = args.drop(2).map(_ + ":9092").mkString(",")
      setKafkaProps(servers)

      try {
        val awsCredentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        println("Basic creds setup")
        val amazonS3Client = new AmazonS3Client(awsCredentials)
        println("Client setup")
        val consumer = new KafkaConsumer(props)
        val topics = List("DRONE")
        try {
          consumer.subscribe(topics.asJava);
          val buffer = ListBuffer[String]();
          while (true) {
            val records = consumer.poll(10)

            // We use a forEach loop to look for each record
            val intermed = records.iterator().asScala;
            val next = intermed.map(x => x.value().asInstanceOf[String]);
            buffer ++= next.toList;

            if (buffer.length >= 100000) {
              upload(amazonS3Client, buffer.toList.mkString("\n"));
              buffer.clear();
            }
          }
        }catch{
          case e:Exception => e.printStackTrace()
        }finally {
          consumer.close()
        }

        
      } catch {
        case ase: AmazonServiceException => System.err.println("Exception: " + ase.toString)
        case ace: AmazonClientException => System.err.println("Exception: " + ace.toString) 
      }
    }
  }
}
