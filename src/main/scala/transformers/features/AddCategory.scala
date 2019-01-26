package salespred.transformers.features

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import org.apache.spark.ml.Transformer

import salespred.SparkWrapper
import salespred.utils.FileUtils

class AddCategory()(implicit spark: SparkWrapper, files: FileUtils) extends Transformer {
    private val categoryDataPath = "/hdfs/salespred/item_categories_fixed.csv"
    private lazy val categoryData = files.readCSV(categoryDataPath, "category")

    override def transform(df: DataFrame): DataFrame = {
        df.join(
            categoryData,
            col("items.item_category_id") === col("category.item_category_id"),
            "left"
        )
    }
}