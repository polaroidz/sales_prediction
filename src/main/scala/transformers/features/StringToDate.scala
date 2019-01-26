package salespred.transformers.features

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import org.apache.spark.ml.Transformer

import salespred.SparkWrapper

class StringToDate()(implicit spark: SparkWrapper) extends Transformer {

    override def transform(df: DataFrame): DataFrame = {
        df.withColumn("date", to_date(col("date"), "dd.MM.yyyy"))
    }

}