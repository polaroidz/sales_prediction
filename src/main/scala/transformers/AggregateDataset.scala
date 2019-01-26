package salespred.transformers

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Transformer

class AggregateDataset()(implicit spark: SparkSession) extends Transformer {

    val uid: String = "AggregateDataset"

    override def transformSchema(schema: StructType): StructType = schema
    override def copy(extra: ParamMap): Transformer = null

    override def transform(df: Dataset[_]): DataFrame = {
        df.groupBy(
            col("date_block_num"), 
            col("shop_id"), 
            col("item_id"))
        .agg(
            first("shop_name").as("shop_name"),
            first("shop_type").as("shop_type"),
            first("city_name").as("city_name"),
            first("lat").as("lat"),
            first("long").as("long"),
            first("item_name").as("item_name"),
            first("item_category_name").as("item_category_name"),
            min("date").as("min_date"),
            max("date").as("max_date"),
            sum("holiday").as("holidays"),
            sum("weekend").as("weekend_sales"),
            round(mean("dayofyear")).as("dayofyear"),
            round(mean("weekofyear")).as("weekofyear"),
            mean("usd_rate").as("usd_rate"),
            mean("item_price").as("avg_item_price"),
            abs(sum("item_cnt_day")).as("item_cnt_month"))
        .orderBy(
            col("date_block_num"),
            col("shop_id"),
            col("item_id"))
    }

}