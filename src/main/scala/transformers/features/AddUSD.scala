package salespred.transformers.features

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Transformer

import salespred.utils.FileUtils

class AddUSD()(implicit spark: SparkSession, files: FileUtils) extends Transformer {
    private val usdPath = "/hdfs/salespred/utils/usd-rub.csv"
    private lazy val usdData = files.readCSV(usdPath, "usd")

    val uid: String = "AddUSD"

    override def transformSchema(schema: StructType): StructType = schema
    override def copy(extra: ParamMap): Transformer = null

    override def transform(df: Dataset[_]): DataFrame = {
        df.join(
            usdData,
            col("df.date") === col("usd.date"),
            "left"
        )
    }
}