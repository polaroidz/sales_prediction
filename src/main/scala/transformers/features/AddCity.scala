package salespred.transformers.features

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import org.apache.spark.ml.Transformer

import salespred.SparkWrapper
import salespred.utils.FileUtils

class AddCity()(implicit spark: SparkWrapper, files: FileUtils) extends Transformer {
    private val cityPath = "/hdfs/salespred/utils/city_name_eng.csv"
    private lazy val cityData = files.readCSV(cityPath, "category")

    override def transform(df: DataFrame): DataFrame = {
        df.join(
            cityData,
            col("df.shop_id") === col("city.shop_id"),
            "left"
        )
    }
}