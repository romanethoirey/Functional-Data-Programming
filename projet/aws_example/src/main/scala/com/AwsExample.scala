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

/**
 * Example Scala code that uses an AWS client to do two things:
 * - Upload data by using an input stream
 * - Read data from an AWS file
 */
object AwsExample {
  val BUCKET_NAME = "fdpprjoect"
  val FILE_NAME = "dummyfile5.txt"

  def upload(client: AmazonS3Client, content:String) : Unit = {

    // create a new bucket
    // client.createBucket(BUCKET_NAME)
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

  def read(client: AmazonS3Client) : Unit = {
    // download file and read line by line
    val obj = client.getObject(BUCKET_NAME, FILE_NAME)
    val reader = new BufferedReader(new InputStreamReader(obj.getObjectContent()))
    var line = reader.readLine
    while (line!=null) {
      println(line)
      line = reader.readLine
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

        val lines = List("lorem ipsum", "dolor sit amet")

        upload(amazonS3Client, lines.mkString("\n"))

        read(amazonS3Client)

        
        
      } catch {
        case ase: AmazonServiceException => System.err.println("Exception: " + ase.toString)
        case ace: AmazonClientException => System.err.println("Exception: " + ace.toString) 
      }
    }
  }
}
