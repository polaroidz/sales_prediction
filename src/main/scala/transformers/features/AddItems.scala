package salespred.transformers.features

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import org.apache.spark.ml.Transformer

import salespred.SparkWrapper
import salespred.utils.FileUtils

class AddItems()(implicit spark: SparkWrapper, files: FileUtils) extends Transformer {
    private val itemsDataPath = "/hdfs/salespred/items.csv"
    private lazy val itemsData = files.readCSV(itemsDataPath, "items")

    override def transform(df: DataFrame): DataFrame = {
        df.join(
            itemsData,
            col("df.item_id") === col("items.item_id"),
            "left"
        )
    }
}