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
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe



/**
 * Example Scala code that uses an AWS client to do two things:
 * - Upload data by using an input stream
 * - Read data from an AWS file
 */
object KafkaToAws {
  val BUCKET_NAME = ""
  val FILE_NAME = "dummyfile5.txt"

  def upload(client: AmazonS3Client, content:String) : Unit = {

    // create a new bucket
    client.createBucket(BUCKET_NAME)
    val scratchFile = File.createTempFile("prefix", "suffix");
    try {
      val bw = new BufferedWriter(new FileWriter(scratchFile))
      bw.write(content)
      bw.close()
      val putObjectRequest = new PutObjectRequest(BUCKET_NAME, FILE_NAME, scratchFile);
      val putObjectResult = client.putObject(putObjectRequest);
    
    } finally {
      if(scratchFile.exists()) {
        scratchFile.delete();
      }
    }

  }

  def getLines() : List[String] = {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    
    val topics = Array("drone")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD { rdd =>
      // What is in this rdd variable?
      println(rdd)
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //rdd.foreachPartition { iter =>
      //  val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
      //  println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    }

  }

  def main(args: Array[String]) : Unit = {
    println(args.length)
    if (args.length != 2) {
      println("Usage: ./AwsExample AWS_ACCESS_KEY AWS_SECRET_KEY")
    } else {
      val AWS_ACCESS_KEY = args(0)
      val AWS_SECRET_KEY =  args(1)
      try {
        val awsCredentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        println("Basic creds setup")
        val amazonS3Client = new AmazonS3Client(awsCredentials)
        println("Client setup")

        val lines = getLines()



        //upload(amazonS3Client, lines.mkString("\n"))

        
      } catch {
        case ase: AmazonServiceException => System.err.println("Exception: " + ase.toString)
        case ace: AmazonClientException => System.err.println("Exception: " + ace.toString) 
      }
    }
  }
}
