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

class FeatureSelection()(implicit spark: SparkSession, files: FileUtils) {
    import spark.implicits._

    private val vectorDataPath = "/hdfs/salespred/output/sales_vectorized.parquet"
    private val df = files.readParquet(vectorDataPath, "df")

    private val disassemblerPath = "/hdfs/salespred/models/Disassembler"
    private val selectedDataPath = "/hdfs/salespred/output/sales_selected_features.csv"
    private val selectedVecDataPath = "/hdfs/salespred/output/sales_selected_features.parquet"

    def run(args: Array[String]) = {
        val disassembler = new org.apache.spark.ml.feature.VectorDisassembler()
            .setInputCol("features")

        disassembler.write.overwrite.save(disassemblerPath)
        
        val disassembledDf = disassembler.transform(df)
            .drop("features")

        val model = new GradientBoosting().getModel
        
        val columns = disassembledDf.columns

        val featureImportances = model.featureImportances.toArray
        val features = new mutable.ArrayBuffer[Tuple2[String, Double]]()

        for (i <- (0 until featureImportances.size)) {
            features += new Tuple2(columns(i + 5), featureImportances(i))
        }

        val selectedFeatures = features.filter(_._2 > 0).map(_._1).toArray
        val selectedColumns = Array("date_block_num", "date", "shop_id", "item_id") ++ selectedFeatures ++ Array("item_cnt_month")
        val selectedColumns2 = selectedColumns.map(e => col(s"`${e}`"))

        val selectedDf = disassembledDf.select(selectedColumns2: _*)

        selectedDf.write
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .option("mode", "overwrite")
          .save(selectedDataPath)
        
        val assembler = new VectorAssembler()
            .setInputCols(selectedFeatures)
            .setOutputCol("features")
        
        val outputDf = assembler.transform(selectedDf)

        output.write
            .option("compression","none")
            .mode("overwrite")
            .save(selectedVecDataPath)
    }
}