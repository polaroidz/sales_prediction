package salespred.tasks

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import salespred.SparkWrapper
import salespred.utils.FileUtils

import salespred.transformes.FilterDataset
import salespred.transformes.EnrichDataset
import salespred.transformes.AggregateDataset

import scala.collection.mutable

class DatasetFormatting()(implicit spark: SparkWrapper, files: FileUtils) {
    private val trainingDataPath = "/hdfs/salespred/sales_train_v2.csv"
    private lazy val trainingData = files.readCSV(trainingDataPath, "df")

    def run(args: Array[String]) = {
        val stages = new mutable.ArrayBuffer[PipelineStage]()

        stages += new FilterDataset()
        stages += new EnrichDataset()
        stages += new AggregateDataset()

        val pipeline = new Pipeline().setStages(stages.toArray())

        val output = pipeline.transform(trainingData)

        output.show(10)
    }
}