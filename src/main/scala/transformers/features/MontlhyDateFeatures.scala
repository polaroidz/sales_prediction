package salespred.transformers.features

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Transformer

class MonthlyDateFeatures()(implicit spark: SparkSession) extends Transformer {

    val uid: String = "MonthlyDateFeatures"

    override def transformSchema(schema: StructType): StructType = schema
    override def copy(extra: ParamMap): Transformer = null

    override def transform(df: Dataset[_]): DataFrame = {
        df
        .withColumn("year", year(col("max_date")))
        .withColumn("month", month(col("max_date")))
        // Distance between the first and last item sold on that month
        .withColumn("days_range", datediff(col("max_date"), col("min_date")))
    }

}