import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

val spark = SparkSession.builder.appName("WineDataAnalysis").getOrCreate()
import spark.implicits._

// Load the data
val winesDF = spark.read.option("header", "true").option("inferSchema", "true").csv("CleanedTable.csv")

// Data processing: converting Price to numeric and Vintage to age
val processedDF = winesDF
    .withColumn("Points", $"Points".cast(DoubleType))
    .withColumn("Price", regexp_replace($"Price", "\\$", "").cast(DoubleType))
    .withColumn("VintageYear", split($"Vintage", "/")(2).cast(IntegerType))
    .withColumn("Age", (lit(2023) - $"VintageYear").cast(DoubleType))
    .drop("VintageYear")

// Display the table
val aftercount = processedDF.count()
println(s"Table after and its count: $aftercount")
processedDF.printSchema()
processedDF.show()

// Calculating statistics for 'Points', 'Price', 'Age'
val numericalCols = Seq("Points", "Price", "Age")

numericalCols.foreach { col =>
  val meanVal = processedDF.agg(mean(col).alias("Mean")).first().getAs[Double]("Mean")
  val medianVal = processedDF.stat.approxQuantile(col, Array(0.5), 0.01).head
  val modeVal = processedDF.groupBy(col).count().orderBy(desc("count")).first().get(0)
  val stdDevVal = processedDF.agg(stddev(col).alias("Standard Deviation")).first().getAs[Double]("Standard Deviation")
  val minVal = processedDF.agg(min(col).alias("Min")).first().getAs[Double]("Min")
  val maxVal = processedDF.agg(max(col).alias("Max")).first().getAs[Double]("Max")

  println(s"Statistics for $col:")
  println(s"Mean: $meanVal")
  println(s"Median: $medianVal")
  println(s"Mode: $modeVal")
  println(s"Standard Deviation: $stdDevVal")
  println(s"Min: $minVal")
  println(s"Max: $maxVal")
  println()
}

processedDF.write.option("header", "true").csv("ProcessedWines.csv")
