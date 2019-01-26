package salespred.transformers.features

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Transformer

import salespred.utils.FileUtils

class AddGeoCoordinates()(implicit spark: SparkSession, files: FileUtils) extends Transformer {
    private val geoPath = "/hdfs/salespred/utils/shop_coordinates.csv"
    private lazy val geoData = files.readCSV(geoPath, "geo")

    val uid: String = "AddGeoCoordinates"

    override def transformSchema(schema: StructType): StructType = schema
    override def copy(extra: ParamMap): Transformer = null

    override def transform(df: Dataset[_]): Dataset[_] = {
        df.join(
            geoData,
            col("df.shop_id") === col("geo.shop_id"),
            "left"
        )
    }
}