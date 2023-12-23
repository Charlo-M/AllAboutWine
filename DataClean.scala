import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

val spark = SparkSession.builder.appName("WineDataAnalysis").getOrCreate()
import spark.implicits._

// Load the data
val winesDF = spark.read.option("header", "true").option("inferSchema", "true").csv("Wines.csv")
val beforecount = winesDF.count()
println(s"Table before conducting data profiling and cleaning and its count: $beforecount")
winesDF.printSchema()
winesDF.show()

// Removing rows with null values
val dfNoNull = winesDF.na.drop()


// Display the table
val aftercount = dfNoNull.count()
println(s"Table after conducting data profiling and cleaning and its count: $aftercount")
dfNoNull.printSchema()
dfNoNull.show()

dfNoNull.write.option("header", "true").csv("CleanedTable.csv")