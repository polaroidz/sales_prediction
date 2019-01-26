package salespred.transformers.features

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Transformer

import salespred.utils.FileUtils

class AddCity()(implicit spark: SparkSession, files: FileUtils) extends Transformer {
    private val cityPath = "/hdfs/salespred/utils/city_name_eng.csv"
    private lazy val cityData = files.readCSV(cityPath, "city")

    val uid: String = "AddCity"

    override def transformSchema(schema: StructType): StructType = schema
    override def copy(extra: ParamMap): Transformer = null

    override def transform(df: Dataset[_]): DataFrame = {
        df.join(
            cityData,
            col("df.shop_id") === col("city.shop_id"),
            "left"
        )
    }
}