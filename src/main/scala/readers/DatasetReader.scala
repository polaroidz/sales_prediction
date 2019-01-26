package salespred.readers

import salespred.utils.FileUtils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DatasetReader {
    private val trainingDataPath = "/hdfs/salespred/sales_train_v2.csv"
    private val categoryDataPath = "/hdfs/salespred/item_categories_fixed.csv"
    private val itemsDataPath = "/hdfs/salespred/items.csv"
    private val shopsDataPath = "/hdfs/salespred/shops.csv"

    private val outputTrainingPath = "/hdfs/salespred/output/sales_aggregated.csv"

    def trainingData = FileUtils.readCSV(trainingDataPath, "df")
    def categoryData = FileUtils.readCSV(categoryDataPath, "category")
    def itemsData = FileUtils.readCSV(itemsDataPath, "items")
    def shopsData = FileUtils.readCSV(shopsDataPath, "shops")
    def loadSavedTrainingData = FileUtils.readCSV(outputTrainingPath, "df")

}