package salespred.transformers

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import org.apache.spark.ml.Transformer

import salespred.SparkWrapper

class FilterDataset()(implicit spark: SparkWrapper) extends Transformer {

    override def transform(df: DataFrame): DataFrame = {
        df.filter(col("item_cnt_day") < 0)
    }

}