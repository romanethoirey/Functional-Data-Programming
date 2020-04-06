// Databricks notebook source
Importing the file from S3 bucket and create data frames

// COMMAND ----------

val AccessKey = "AKIAWXKL5AG6VEWNCU6P"
// Encode the Secret Key as that can contain "/"
val SecretKey = "Qe0SvR4oIS1+Yw69oMP0TH5N0P6uxpdbc3mbw6AG".replace("/", "%2F")
val AwsBucketName = "fdpprojectcreatingnewbucket"
val MountName = "tst2"

dbutils.fs.mount(s"s3a://$AccessKey:$SecretKey@$AwsBucketName", s"/mnt/$MountName")
display(dbutils.fs.ls(s"/mnt/$MountName"))

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType,TimestampType }
import org.apache.spark.sql.types._

val schema = StructType(
  List(
    StructField("Summons Number", IntegerType, true),
    StructField("Plate ID", IntegerType, true),
    StructField("Registration State", StringType, true),
    StructField("Plate Type", StringType, true),
    StructField("Issue Date", TimestampType, true),
    StructField("Violation Code", StringType, true),
    StructField("Vehicle Body Type", StringType, true),
    StructField("Vehicle Make", StringType, true),
    StructField("Issuing Agency", StringType, true),
    StructField("Street Code1", StringType, true),
    StructField("Street Code2", StringType, true),
    StructField("Street Code3", StringType, true),
    StructField("Vehicle Expiration Date", TimestampType, true),
    StructField("Violation Location", StringType, true),
    StructField("Violation Precinct", StringType, true),
    StructField("Issuer Precinct", StringType, true),
    StructField("Issuer Code", StringType, true),
    StructField("issuer Command", IntegerType, true),
    StructField("Issuer Squad", StringType, true),
    StructField("Violation Time", StringType, true),
    StructField("Time First Observed", TimestampType, true),
    StructField("Violation County", StringType, true),
    StructField("Violation In Front Of Or Opposite", StringType, true),
    StructField("House Number", StringType, true),
    StructField("Street Name", StringType, true),
    StructField("Intersecting Street", StringType, true),
    StructField("Date First Observed", StringType, true),
    StructField("Law Section", StringType, true),
    StructField("Sub Division", StringType, true),
    StructField("Violation Legal Code", StringType, true),
    StructField("Days Parking In Effect  ", StringType, true),
    StructField("From Hours In Effect", StringType, true),
    StructField("To Hours In Effect", StringType, true),
    StructField("Vehicle Color", StringType, true),
    StructField("Unregistered Vehicle?", StringType, true),
    StructField("Vehicle Year", StringType, true),
    StructField("Meter Number", StringType, true),
    StructField("Feet From Curb", IntegerType, true),
    StructField("Violation Post Code", IntegerType, true),
    StructField("Violation Description", StringType, true),
    StructField("No Standing or Stopping Violation", StringType, true),
    StructField("Hydrant Violation", StringType, true),
    StructField("Double Parking Violation", StringType, true),
    StructField("BBL", StringType, true),
    StructField("NTA", StringType, true) 
  )
)

// COMMAND ----------

val dftst = spark.read.option("header", "true").schema(schema).csv("dbfs:/mnt/tst/2020-04-05_17_01_seed_23119")
val df1=spark.read.option("header", "true").schema(schema).csv("dbfs:/mnt/tst2/2020-04-05_17_04_seed_12061")


// COMMAND ----------

display(df1)

// COMMAND ----------

Data processing before graphe

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.graphframes._

// COMMAND ----------

import org.apache.spark.sql.functions._ 
import org.apache.spark.sql.expressions.Window


val dfId = dftst.withColumn("id", row_number().over(Window.orderBy(lit(1))))


val dffId=df1.withColumn("id", row_number().over(Window.orderBy(lit(1))))

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.functions._

val newdff1 = dfId.select(col("*"), substring(col("Violation Time"), 0, 4).as("Violation Time hours"))
val newdff2 = newdff1.select(col("*"), substring(col("Violation Time"), 5, 5).as("Violation Time A/P"))
val newdff3 = newdff2.select(col("*"), substring(col("Time First Observed"), 0, 4).as("Time First Observed hours"))
val newdff4 = newdff3.select(col("*"), substring(col("Time First Observed"), 5, 5).as("Time First Observed A/P"))
val newdff5 = newdff4.select(col("*"), substring(col("Issue Date"), 0, 10).as("IssueDateSplit"))



// COMMAND ----------

val dfVertices = dfId
  .distinct()

val dffEdges = newdff5
  .withColumnRenamed("Registration State", "src")
  .withColumnRenamed("Street Name", "dst")




// COMMAND ----------

Create graphe object graphe and plot features

// COMMAND ----------

val g = GraphFrame(dfVertices,dffEdges)

// COMMAND ----------

display(g.vertices)

// COMMAND ----------


display(g.edges)

// COMMAND ----------

Who has cause the most issues 

// COMMAND ----------

//mark of car that have the most been register
val state = g.vertices.groupBy("Vehicle Make").count()
display(state)

// COMMAND ----------

//Identify which plate have been the moste register and which body type
//PAS=Passenger, OMT=Taxi, OMS=Rental
// SBN=Ambulance,Bus Carryall, Coach,Hearse,Micro,Bus Station,Bus Station Wagon, Travelall Wagon
//SDN=Sedan
val passenger = g.vertices.groupBy("Vehicle Body Type","Plate Type").count()
display(passenger)

// COMMAND ----------

Where does we have the most issues

// COMMAND ----------

//Find the most register county
//Manhattan (NY), Brooklyn (K), Bronx (Bx), Queens (Qn), and Staten Island (Rich)
val nbh = g
  .edges
  .groupBy("Violation County")
  .count()
  .orderBy(desc("count"))

display(nbh)




// COMMAND ----------

// which precinct by county have the most violation issue 
//link to a representation of NY precinct on map :https://googlesamp.blogspot.com/2014/07/nypd-precinct-map.html
val district = g
  .edges
  .groupBy("Violation Precinct","Violation County")
  .count()
  .orderBy(desc("Violation Precinct"))

display(district)

// COMMAND ----------

//Which street by precint they have to keep an eye on
val register = g
  .edges
  .groupBy("Violation Precinct","dst")
  .count()
  .orderBy(desc("Violation Precinct"))

display(register)

// COMMAND ----------

When do the issue happen the most 

// COMMAND ----------

//“Time 1st Observed” and “Date 1st Observed” are required for violations for which it is necessary to establish the length of time the vehicle is in a Time Limited Zone before a violation may be issued
//Violation Time 

val Vtime = g
  .edges
  .groupBy("Violation Time hours","Violation Time A/P")
  .count()
  .orderBy(desc("count"))

display(Vtime)



// COMMAND ----------

Whate violation is the most register 

// COMMAND ----------

//New york Vehicle and Traffic Law (view all law section : http://www.nyc.gov/html/dot/downloads/pdf/trafrule.pdf)
//408 part for Parking, Stopping, Standing
//sub division h1 On-street and off-street metered zone Purchasing parking time
//c Violation of posted no standing rules prohibited
//d1 Violation of posted no parking rules prohibited on Street cleaning place
//F1 Double parking

val law = g
  .vertices
  .groupBy("Sub Division","Law Section")
  .count()
  .orderBy(desc("count"))

display(law)

// COMMAND ----------

// view all NY violation code possible : https://data.ny.gov/widgets/ncbg-6agr
// 14 - NO STANDING-DAY/TIME LIMITS 65$
//21- No parking where parking is not allowed by sign, street marking or traffic control device. $65
//38- FAIL TO DSPLY MUNI METER RECPT 65$
//46- "double parking" $115

val VCode = g
  .edges
  .groupBy("Violation Code")
  .count()
  .orderBy(desc("count"))

display(VCode)

// COMMAND ----------

//how to decripte the data set
//http://firetesttaking.com/pdfs/auc/162.pdf
