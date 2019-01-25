package salespred

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object UtilsReader {

    private val shopGeoDataPath = "/hdfs/salespred/utils/shop_coordinates.csv"
    private val shopCityNamesDataPath = "/hdfs/salespred/utils/city_name_eng.csv"

    def shopGeoData =
        SparkWrapper.get.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("model", "DROPMALFORMED")
            .load(shopGeoDataPath)
            .as("geo")

    def shopCityData =
        SparkWrapper.get.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("model", "DROPMALFORMED")
            .load(shopCityNamesDataPath)
            .as("city")

}