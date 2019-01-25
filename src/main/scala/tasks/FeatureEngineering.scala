package salespred.tasks

import salespred.readers.DatasetReader

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object FeatureEngineering {

    def addDateFeatures(df: DataFrame):DataFrame = df
        .withColumn("year", year(col("max_date")))
        .withColumn("month", month(col("max_date")))
        // Distance between the first and last item sold on that month
        .withColumn("days_range", datediff(col("max_date"), col("min_date")))


    def run(args: Array[String]) {
        val df = DatasetReader.loadSavedTrainingData

        // Date Features
        val dateDF = addDateFeatures(df)

        dateDF.select(
            col("date_block_num"),
            col("year"),
            col("month"),
            col("days_range")
        ).show(10)
    }
}