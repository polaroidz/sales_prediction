package salespred.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

class FileUtils()(implicit spark: SparkSession) {

    def readCSV(path: String, alias:String): DataFrame =
        spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("model", "DROPMALFORMED")
            .load(path)
            .as(alias)

}