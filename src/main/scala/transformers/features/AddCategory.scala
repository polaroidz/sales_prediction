package salespred.transformers.features

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Transformer

import salespred.utils.FileUtils

class AddCategory()(implicit spark: SparkSession, files: FileUtils) extends Transformer {
    private val categoryDataPath = "/hdfs/salespred/item_categories_fixed.csv"
    private lazy val categoryData = files.readCSV(categoryDataPath, "category")

    val uid: String = "AddCategory"

    override def transformSchema(schema: StructType): StructType = schema
    override def copy(extra: ParamMap): Transformer = null

    override def transform(df: Dataset[_]): DataFrame = {
        df.join(
            categoryData,
            col("items.item_category_id") === col("category.item_category_id"),
            "left"
        )
    }
}