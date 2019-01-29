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
import salespred.transformers.ml.RegressionModel
import salespred.transformers.ml.GradientBoosting

import scala.collection.mutable

class MakingPredictions()(implicit spark: SparkSession, files: FileUtils) {
    import spark.implicits._

    private val vectorDataPath = "/hdfs/salespred/output/sales_vectorized.parquet"
    private val df = files.readParquet(vectorDataPath, "df")

    def run(args: Array[String]) = {
        val model = new GradientBoosting()
            .fit(df)
        
        val output = model.transform(df)

        output.show(10)
    }
}