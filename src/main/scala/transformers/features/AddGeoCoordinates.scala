package salespred.transformers.features

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import org.apache.spark.ml.Transformer

import salespred.SparkWrapper
import salespred.utils.FileUtils

class AddGeoCoordinates()(implicit spark: SparkWrapper, files: FileUtils) extends Transformer {
    private val geoPath = "/hdfs/salespred/utils/shop_coordinates.csv"
    private lazy val geoData = files.readCSV(geoPath, "geo")

    override def transform(df: DataFrame): DataFrame = {
        df.join(
            geoData,
            col("df.shop_id") === col("geo.shop_id"),
            "left"
        )
    }
}