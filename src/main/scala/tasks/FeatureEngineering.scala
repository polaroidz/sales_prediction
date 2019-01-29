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

import salespred.transformers.models.FeaturesEncoder
import salespred.transformers.models.NumericalScaler
import salespred.transformers.models.CategoricalEncoder
import salespred.transformers.models.NLPModel

import scala.collection.mutable

class FeatureEngineering()(implicit spark: SparkSession, files: FileUtils) {
    import spark.implicits._

    private val aggDataPath = "/hdfs/salespred/output/sales_aggregated.csv"
    private val vectorDataPath = "/hdfs/salespred/output/sales_vectorized.parquet"
    private val df = files.readCSV(aggDataPath, "df")

    def run(args: Array[String]) = {

        new NumericalScaler().fit(df)
        new CategoricalEncoder().fit(df)
        new NLPModel().fit(df)

        val model = new FeaturesEncoder().fit(df)        
        val output = model.transform(df)

        output.show(10)
        println(output.count)

        output.write
            .option("compression","none")
            .mode("overwrite")
            .save(vectorDataPath)
    }
}