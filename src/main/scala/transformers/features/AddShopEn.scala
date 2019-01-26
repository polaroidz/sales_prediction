package salespred.transformers.features

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Transformer

import salespred.utils.FileUtils

class AddShopEn()(implicit spark: SparkSession, files: FileUtils) extends Transformer {
    private val shopsDataPath = "/hdfs/salespred/utils/shops_translated.csv"
    private lazy val shopsData = files.readCSV(shopsDataPath, "shops")

    val uid: String = "AddShopEn"

    override def transformSchema(schema: StructType): StructType = schema
    override def copy(extra: ParamMap): Transformer = null

    override def transform(df: Dataset[_]): DataFrame = {
        df.join(
            shopsData,
            col("df.shop_id") === col("shops.shop_id"),
            "left"
        )
    }

}