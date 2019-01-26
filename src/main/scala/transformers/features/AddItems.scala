package salespred.transformers.features

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Transformer

import salespred.utils.FileUtils

class AddItems()(implicit spark: SparkSession, files: FileUtils) extends Transformer {
    private val itemsDataPath = "/hdfs/salespred/items.csv"
    private lazy val itemsData = files.readCSV(itemsDataPath, "items")

    val uid: String = "AddItems"

    override def transformSchema(schema: StructType): StructType = schema
    override def copy(extra: ParamMap): Transformer = null

    override def transform(df: Dataset[_]): DataFrame = {
        df.join(
            itemsData,
            col("df.item_id") === col("items.item_id"),
            "left"
        )
    }
}