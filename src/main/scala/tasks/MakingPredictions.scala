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

import org.apache.spark.ml.evaluation.RegressionEvaluator

import scala.collection.mutable

class MakingPredictions()(implicit spark: SparkSession, files: FileUtils) {
    import spark.implicits._

    private val vectorDataPath = "/hdfs/salespred/output/sales_vectorized.parquet"
    private val df = files.readParquet(vectorDataPath, "df")

    def run(args: Array[String]) = {
        val model = new GradientBoosting().getModel
        
        val featureImportances = model.featureImportances.toArray
        val features = new mutable.ArrayBuffer[Tuple2[Int, Double]]()

        for (i <- (0 until featureImportances.size)) {
            features += new Tuple2(i, featureImportances(i))
        }

        val selectedFeatures = features.filter(_._2 > 0).map(_._1).toArray

        val disassembler = new org.apache.spark.ml.feature.VectorDisassembler()
            .setInputCol("features")
        
        val disassembledDf = disassembler.transform(df)

        disassembledDf.show(10)

    }
}