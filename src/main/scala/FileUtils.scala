package salespred

import org.apache.spark.sql.DataFrame

object FileUtils {

    def readCSV(path: String, alias:String): DataFrame =
        SparkWrapper.get.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("model", "DROPMALFORMED")
            .load(path)
            .as(alias)

}