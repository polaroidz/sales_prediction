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

import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.regression.GBTRegressionModel

import org.apache.spark.ml.evaluation.RegressionEvaluator

class GradientBoosting()(implicit spark: SparkSession) extends Model {

    val uid: String = "GradientBoosting"

    private val maxIter = 3

    private val featuresCol = "features"
    private val labelCol = "item_cnt_month"

    private val modelPath = s"/hdfs/salespred/models/${uid}"

    override def transformSchema(schema: StructType): StructType = schema
    override def copy(extra: ParamMap) = defaultCopy(extra)

    override def transform(ds: Dataset[_]): DataFrame = {
        val model = GBTRegressionModel.read.load(modelPath)

        model.transform(ds)
    }

    def getModel = GBTRegressionModel.read.load(modelPath)

    def fit(ds: Dataset[_]): GradientBoosting = {
        println("Starting training Gradient Boosting with Params")
        println(s"Max Iter: $maxIter")

        val Array(trainingData, testData) = ds.randomSplit(Array(0.7, 0.3))

        val gbt = new GBTRegressor()
            .setFeaturesCol(featuresCol)
            .setLabelCol(labelCol)
            .setMaxIter(maxIter)
            .fit(trainingData)

        gbt.write.overwrite.save(modelPath)

        val predictions = gbt.transform(testData)

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