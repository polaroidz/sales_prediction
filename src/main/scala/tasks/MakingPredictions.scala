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

import scala.collection.mutable

class MakingPredictions()(implicit spark: SparkSession, files: FileUtils) {
    import spark.implicits._

    private val vectorDataPath = "/hdfs/salespred/output/sales_vectorized.parquet"
    private val df = files.readParquet(vectorDataPath, "df")

    def run(args: Array[String]) = {
        df.show(10)
    }
}