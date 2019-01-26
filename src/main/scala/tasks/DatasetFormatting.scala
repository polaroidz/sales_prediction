package salespred.tasks

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import salespred.utils.FileUtils

import salespred.transformers.FilterDataset
import salespred.transformers.EnrichDataset
import salespred.transformers.AggregateDataset

import salespred.models.TrainingData

import scala.collection.mutable

class DatasetFormatting()(implicit spark: SparkSession, files: FileUtils) {
    import spark.implicits._

    private val trainingDataPath = "/hdfs/salespred/sales_train_v2.csv"
    private lazy val trainingData = files.readCSV(trainingDataPath, "df")

    private val outputPath = "/hdfs/salespred/output/sales_aggregated.csv"

    def run(args: Array[String]) = {
        val ds = trainingData.as[TrainingData]

        val stages = new mutable.ArrayBuffer[PipelineStage]()

        stages += new FilterDataset()
        stages += new EnrichDataset()
        //stages += new AggregateDataset()

        val pipeline = new Pipeline().setStages(stages.toArray).fit(ds)

        val output = pipeline.transform(ds)

        output.show(10)

        //output.write
        //  .format("com.databricks.spark.csv")
        //  .option("header", "true")
        //  .option("mode", "overwrite")
        //  .save(outputPath)
    }
}