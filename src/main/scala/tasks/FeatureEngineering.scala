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

import salespred.transformers.models.NumericalScaler
import salespred.transformers.models.CategoricalEncoder

import scala.collection.mutable

class FeatureEngineering()(implicit spark: SparkSession, files: FileUtils) {
    import spark.implicits._

    private val aggDataPath = "/hdfs/salespred/output/sales_aggregated.csv"
    private val df = files.readCSV(aggDataPath, "df")

    def run(args: Array[String]) = {
        // Numerical Variables
        //val numericalScaler = new NumericalScaler()
        //    .fit(df)
        //val output = numericalScaler.transform(df)
        //output.select("scaled_features").show(10)
        // Categorical Variables
        val catEncoder = new CategoricalEncoder()
            .fit(df)
        
        val output = catEncoder.transform(df)
        output.select("categorical_features").show(10)
        // Text Variables

    }
}