package salespred.transformers.features

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import org.apache.spark.ml.Transformer

import salespred.SparkWrapper
import salespred.utils.FileUtils

class AddShop()(implicit spark: SparkWrapper, files: FileUtils) extends Transformer {
    private val shopsDataPath = "/hdfs/salespred/shops.csv"
    private lazy val shopsData = files.readCSV(shopsDataPath, "shops")

    override def transform(df: DataFrame): DataFrame = {
        df.join(
            shopsData,
            col("df.shop_id") === col("shops.shop_id"),
            "left"
        )
    }

}