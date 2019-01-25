package salespred

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DatasetReader {
    private val trainingDataPath = "/hdfs/salespred/sales_train_v2.csv"
    private val categoryDataPath = "/hdfs/salespred/item_categories.csv"
    private val itemsDataPath = "/hdfs/salespred/items.csv"
    private val shopsDataPath = "/hdfs/salespred/shops.csv"

    def trainingData =
        SparkWrapper.get.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("model", "DROPMALFORMED")
            .load(trainingDataPath)
            .as("df")

    def categoryData =
        SparkWrapper.get.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("model", "DROPMALFORMED")
            .load(categoryDataPath)
            .as("category")

    def itemsData =
        SparkWrapper.get.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("model", "DROPMALFORMED")
            .load(itemsDataPath)
            .as("items")

    def shopsData =
        SparkWrapper.get.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("model", "DROPMALFORMED")
            .load(shopsDataPath)
            .as("shops")

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

    def filteredDataset : DataFrame = {
        trainingData
        .filter(col("item_cnt_day") > 0)
    }

    def loadJoinedDataset : DataFrame = {
        filteredDataset
        .join(
            shopsData,
            col("df.shop_id") === col("shops.shop_id"),
            "left"
        )
        .join(
            itemsData,
            col("df.item_id") === col("items.item_id"),
            "left"
        )
        .join(
            categoryData,
            col("items.item_category_id") === col("category.item_category_id"),
            "left"
        )
        .select(
            col("df.date_block_num"),
            col("df.date"),
            col("df.shop_id"),
            col("shops.shop_name"),
            col("df.item_id"),
            col("items.item_name"),
            col("category.item_category_id"),
            col("category.item_category_name"),
            col("df.item_cnt_day"),
            col("df.item_price")
        )
    }


}