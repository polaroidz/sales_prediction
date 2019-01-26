package salespred.tasks

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import salespred.utils.FileUtils

import salespred.transformers.AggregateDataset
import salespred.transformers.features.MonthlyDateFeatures

import salespred.models.TrainingData

import scala.collection.mutable

class SaveAggregatedDataset()(implicit spark: SparkSession, files: FileUtils) {
    import spark.implicits._

    private val richDataPath = "/hdfs/salespred/output/sales_rich.csv"
    private val richData = files.readCSV(richDataPath, "df")

    private val outputPath = "/hdfs/salespred/output/sales_aggregated.csv"

    def run(args: Array[String]) = {
        val stages = new mutable.ArrayBuffer[PipelineStage]()

        stages += new AggregateDataset()
        stages += new MonthlyDateFeatures()

        val pipeline = new Pipeline().setStages(stages.toArray).fit(richData)

        val output = pipeline.transform(richData)

        output.write
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .option("mode", "overwrite")
          .save(outputPath)
    }
}