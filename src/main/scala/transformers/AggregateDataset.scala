package salespred.transformers

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.Transformer

class AggregateDataset()(implicit spark: SparkSession) extends Transformer {

    override def transform(df: DataFrame): DataFrame = {
        df.groupBy(
            col("date_block_num"), 
            col("shop_id"), 
            col("item_id"))
        .agg(
            first("shop_name").as("shop_name"),
            first("city_name").as("city_name"),
            first("lat").as("lat"),
            first("long").as("long"),
            first("item_name").as("item_name"),
            first("item_category_name").as("item_category_name"),
            min("date").as("min_date"),
            max("date").as("max_date"),
            mean("item_price").as("avg_item_price"),
            sum("item_cnt_day").as("item_cnt_month"))
        .orderBy(
            col("date_block_num"),
            col("shop_id"),
            col("item_id"))
    }

}