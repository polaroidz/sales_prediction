package salespred.transformers.ml

import org.apache.spark.sql.functions._

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType

import org.apache.spark.ml.linalg.SQLDataTypes.VectorType 

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.Model

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.PipelineModel

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.LinearRegressionModel

import org.apache.spark.ml.evaluation.RegressionEvaluator

class RegressionModel()(implicit spark: SparkSession) extends Model {

    val uid: String = "RegressionModel"

    private val maxIter = 30
    private val regParam = 0.3
    private val elasticNetParam = 0.8

    private val featuresCol = "features"
    private val labelCol = "item_cnt_month"

    private val modelPath = s"/hdfs/salespred/models/${uid}"

    override def transformSchema(schema: StructType): StructType = schema
    override def copy(extra: ParamMap) = defaultCopy(extra)

    override def transform(ds: Dataset[_]): DataFrame = {
        val model = LinearRegressionModel.read.load(modelPath)

        model.transform(ds)
    }

    def fit(ds: Dataset[_]): RegressionModel = {
        println("Starting training Linear Regression with Params")
        println(s"Max Iter: $maxIter")
        println(s"Reg Param: $regParam")
        println(s"Elastic Net Param: $elasticNetParam")

        val Array(trainingData, testData) = ds.randomSplit(Array(0.7, 0.3))

        val lr = new LinearRegression()
            .setFeaturesCol(featuresCol)
            .setLabelCol(labelCol)
            .setMaxIter(maxIter)
            .setRegParam(regParam)
            .setElasticNetParam(elasticNetParam)
            .fit(trainingData)

        lr.write.overwrite.save(modelPath)

        // Print the coefficients and intercept for linear regression
        println(s"Coefficients: ${lr.coefficients} Intercept: ${lr.intercept}")

        // Summarize the model over the training set and print out some metrics
        val trainingSummary = lr.summary
        println(s"numIterations: ${trainingSummary.totalIterations}")
        println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
        trainingSummary.residuals.show()
        println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
        println(s"r2: ${trainingSummary.r2}")

        val predictions = lr.transform(testData)

        predictions.select("shop_id", "item_id", "date", "features", "item_cnt_month", "prediction")
            .show(10)

        val evaluator = new RegressionEvaluator()
            .setLabelCol("item_cnt_month")
            .setPredictionCol("prediction")
            .setMetricName("rmse")
        
        val rmse = evaluator.evaluate(predictions)
        println("Root Mean Squared Error (RMSE) on test data = " + rmse)

        this
    }

}