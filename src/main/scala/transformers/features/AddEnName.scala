package salespred.transformers.features

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Transformer

import salespred.utils.FileUtils

class AddEnName()(implicit spark: SparkSession, files: FileUtils) extends Transformer {
    private val namesPath = "/hdfs/salespred/utils/item_names_en.csv"
    private lazy val namesData = files.readCSV(namesPath, "names")

    val uid: String = "AddEnName"

    override def transformSchema(schema: StructType): StructType = schema
    override def copy(extra: ParamMap): Transformer = null

    override def transform(df: Dataset[_]): DataFrame = {
        df.join(
            namesData,
            col("df.item_id") === col("names.item_id"),
            "left"
        )
    }
}