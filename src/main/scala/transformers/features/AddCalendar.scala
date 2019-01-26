package salespred.transformers.features

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Transformer

import salespred.utils.FileUtils

class AddCalendar()(implicit spark: SparkSession, files: FileUtils) extends Transformer {
    private val calendarPath = "/hdfs/salespred/utils/calendar.csv"
    private lazy val calendarData = files.readCSV(calendarPath, "calendar")

    val uid: String = "AddCalendar"

    override def transformSchema(schema: StructType): StructType = schema
    override def copy(extra: ParamMap): Transformer = null

    override def transform(df: Dataset[_]): DataFrame = {
        df.join(
            calendarData,
            col("df.date") === col("calendar.date"),
            "left"
        )
    }
}