package salespred.transformers.features

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Transformer

class StringToDate(in: String, out: String)(implicit spark: SparkSession) extends Transformer {

    val uid: String = "StringToDate"

    override def transformSchema(schema: StructType): StructType = schema
    override def copy(extra: ParamMap): Transformer = null

    override def transform(df: Dataset[_]): DataFrame = {
        df.withColumn(out, to_date(col(in), "dd.MM.yyyy"))
    }

}