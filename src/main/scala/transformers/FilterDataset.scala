package salespred.transformers

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Transformer

class FilterDataset()(implicit spark: SparkSession) extends Transformer {

    val uid: String = "FilterDataset"

    override def transformSchema(schema: StructType): StructType = schema
    override def copy(extra: ParamMap): Transformer = null

    override def transform(ds: Dataset[_]): DataFrame = {
       ds.filter(col("item_cnt_day") > 0).toDF
    }

}