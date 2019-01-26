package salespred.tasks

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import salespred.readers.DatasetReader
import salespred.readers.UtilsReader

object DatasetFormatting {
    def loadAggregatedData : DataFrame = {
        val df = loadJoinedDataset
        val dfWithDate = df.withColumn("date", to_date(col("date"), "dd.MM.yyyy"))

        dfWithDate
        .groupBy(
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

    def loadJoinedDataset : DataFrame = {
        filteredDataset
        .join(
            DatasetReader.shopsData,
            col("df.shop_id") === col("shops.shop_id"),
            "left"
        )
        .join(
            DatasetReader.itemsData,
            col("df.item_id") === col("items.item_id"),
            "left"
        )
        .join(
            DatasetReader.categoryData,
            col("items.item_category_id") === col("category.item_category_id"),
            "left"
        )
        .join(
            UtilsReader.shopCityData,
            col("df.shop_id") === col("city.shop_id"),
            "left"
        )
        .join(
            UtilsReader.shopGeoData,
            col("df.shop_id") === col("geo.shop_id"),
            "left"
        )
        .select(
            col("df.date_block_num"),
            col("df.date"),
            col("df.shop_id"),
            col("shops.shop_name"),
            col("city.city_name"),
            col("geo.lat"),
            col("geo.long"),
            col("df.item_id"),
            col("items.item_name"),
            col("category.item_category_id"),
            col("category.item_category_name"),
            col("df.item_cnt_day"),
            col("df.item_price")
        )
    }

    def filteredDataset : DataFrame = {
        DatasetReader.trainingData
        .filter(col("item_cnt_day") > 0)
    }

}