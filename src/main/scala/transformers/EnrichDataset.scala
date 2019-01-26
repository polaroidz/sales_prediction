package salespred.transformers

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import salespred.transformers.features.AddCategory
import salespred.transformers.features.AddCity
import salespred.transformers.features.AddItems
import salespred.transformers.features.AddShop
import salespred.transformers.features.AddGeoCoordinates

import salespred.utils.FileUtils

import scala.collection.mutable

class EnrichDataset()(implicit spark: SparkSession, files: FileUtils) extends Transformer {

    val uid: String = "EnrichDataset"

    override def transformSchema(schema: StructType): StructType = schema
    override def copy(extra: ParamMap): Transformer = null

    override def transform(df: Dataset[_]): DataFrame = {
        val stages = new mutable.ArrayBuffer[PipelineStage]()

        stages += new AddShop()
        stages += new AddItems()
        stages += new AddCategory()
        stages += new AddCity()
        stages += new AddGeoCoordinates()

        val pipeline = new Pipeline().setStages(stages.toArray).fit(df)

        pipeline.transform(df)
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

}