package salespred.tasks

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.StandardScaler

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import salespred.utils.FileUtils

import salespred.transformers.AggregateDataset

import scala.collection.mutable

class FeatureEngineering()(implicit spark: SparkSession, files: FileUtils) {
    import spark.implicits._

    private val aggDataPath = "/hdfs/salespred/output/sales_aggregated.csv"
    private val df = files.readCSV(aggDataPath, "df")

    private val numericalVariables = Array(
        "lat",
        "long",
        "holidays",
        "weekend_sales",
        "dayofyear",
        "weekofyear",
        "usd_rate",
        "days_range",
        "avg_item_price"
    )

    def run(args: Array[String]) = {
        // Numerical Variables
        val numericalAssembler = new VectorAssembler()
            .setInputCols(numericalVariables)
            .setOutputCol("numerical_variables")

        val numericalDF = numericalAssembler.transform(df)

        numericalDF.select("numerical_variables").show(10)

        val stdScaler = new StandardScaler()
            .setInputCol("numerical_variables")
            .setOutputCol("scaled_variables")
            .setWithStd(true)
            .setWithMean(false)
            .fit(numericalDF)

        val scaledData = stdScaler.transform(numericalDF)

        scaledData.select("scaled_variables").show(10)

        // Categorical Variables

        // Text Variables

    }
}