package salespred

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object UtilsReader {

    private val shopGeoDataPath = "/hdfs/salespred/utils/shop_coordinates.csv"
    private val shopCityNamesDataPath = "/hdfs/salespred/utils/city_name_eng.csv"

    def shopGeoData = FileUtils.readCSV(shopGeoDataPath, "geo")
    def shopCityData = FileUtils.readCSV(shopCityNamesDataPath, "city")

}