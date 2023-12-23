import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor, RandomForestRegressionModel, LinearRegressionModel}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator

val spark = SparkSession.builder.appName("WineModeling").getOrCreate()
import spark.implicits._

// Load the processed data
val processedDF = spark.read.option("header", "true").option("inferSchema", "true").csv("ProcessedWines.csv")

// Preparing data for modeling
val categoricalCols = Array("Country", "Province", "Variety", "Winery", "Vintage")
val indexers = categoricalCols.map(c => new StringIndexer()
    .setInputCol(c)
    .setOutputCol(c + "_Indexed")
    .setHandleInvalid("keep"))
val featureCols = categoricalCols.map(_ + "_Indexed") ++ Array("Points", "Age")
val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")

val Array(trainingData, testData) = processedDF.randomSplit(Array(0.8, 0.2))

// Define Linear Regression model
val lr = new LinearRegression().setLabelCol("Price").setFeaturesCol("features")

// Define Random Forest model
val rf = new RandomForestRegressor()
  .setLabelCol("Price")
  .setFeaturesCol("features")
  .setMaxBins(5000) 
  
// Building pipelines
val lrPipeline = new Pipeline().setStages(indexers ++ Array(assembler, lr))
val rfPipeline = new Pipeline().setStages(indexers ++ Array(assembler, rf))

// Fit the models
val lrModel = lrPipeline.fit(trainingData)
val rfModel = rfPipeline.fit(trainingData)

// Make predictions
val lrPredictions = lrModel.transform(testData)
val rfPredictions = rfModel.transform(testData)

// Evaluate the models
val evaluator = new RegressionEvaluator().setLabelCol("Price")
val lrMetrics = evaluator.evaluate(lrPredictions)
val rfMetrics = evaluator.evaluate(rfPredictions)

println(s"Linear Regression Evaluation Metrics: $lrMetrics")
println(s"Random Forest Evaluation Metrics: $rfMetrics")

// Extracting feature importance from Random Forest model
val rfFeatureImportance = rfModel.stages.last.asInstanceOf[RandomForestRegressionModel].featureImportances
val rfFeatureNames = assembler.getInputCols
println("Feature Importance for Random Forest model:")
rfFeatureNames.zip(rfFeatureImportance.toArray).sorted.reverse.foreach { case (feature, importance) =>
  println(s"$feature: $importance")
}

// Extracting coefficients from Linear Regression model
val lrCoefficients = lrModel.stages.last.asInstanceOf[LinearRegressionModel].coefficients.toArray
val lrFeatureNames = assembler.getInputCols
println("Coefficients for Linear Regression model:")
lrFeatureNames.zip(lrCoefficients).foreach { case (feature, coeff) =>
  println(s"$feature: $coeff")
}

spark.stop()

