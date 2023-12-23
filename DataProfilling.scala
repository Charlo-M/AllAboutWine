import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

val csv = "Wines.csv"
val data = spark.read.format("csv").option("header", "true").csv(csv)

val columnsToExclude = Seq("Winery", "County", "Designation")
val totalLines = data.count()
println(s"Total lines in the CSV: $totalLines")

val columnsToDisplay = data.columns.filterNot(columnsToExclude.contains)
val dfSelected = data.select(columnsToDisplay.map(col): _*)

val distinctCounts = columnsToDisplay.map(colName => {
  val distinctCount = dfSelected.select(colName).distinct().count()
  (colName, distinctCount)
})

dfSelected.show()
distinctCounts.foreach { case (colName, count) =>
  println(s"Distinct values for $colName: $count")
}

val dfNoNull = dfSelected.na.drop()


val columnsToDisplayAfterNullDrop = dfNoNull.columns.filterNot(columnsToExclude.contains)
val dfSelectedAfterNullDrop = dfNoNull.select(columnsToDisplayAfterNullDrop.map(col): _*)

val distinctCountsAfterNullDrop = columnsToDisplayAfterNullDrop.map(colName => {
  val distinctCount = dfSelectedAfterNullDrop.select(colName).distinct().count()
  (colName, distinctCount)
})

dfSelectedAfterNullDrop.show()
distinctCountsAfterNullDrop.foreach { case (colName, count) =>
  println(s"Distinct values for $colName after null drop: $count")
}
