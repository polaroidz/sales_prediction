package salespred.utils

import salespred.SparkWrapper
import org.apache.spark.sql.DataFrame

class FileUtils()(implicit spark: SparkWrapper) {

    def readCSV(path: String, alias:String): DataFrame =
        spark.get.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("model", "DROPMALFORMED")
            .load(path)
            .as(alias)

}