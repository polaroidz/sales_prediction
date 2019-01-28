package salespred.transformers.models

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.Model

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.PipelineModel

import org.apache.spark.ml.feature.VectorAssembler

import scala.collection.mutable

class FeaturesEncoder()(implicit spark: SparkSession) extends Model {

    val uid: String = "FeaturesEncoder"

    private val features = Array(
        "scaled_features",
        "categorical_features",
        "nlp_features"
    )
    val outputCol = "features"

    private var model: PipelineModel = _

    override def transformSchema(schema: StructType): StructType = schema
        .add(outputCol, VectorType)

    override def copy(extra: ParamMap) = defaultCopy(extra)

    override def transform(ds: Dataset[_]): DataFrame = {
        var output = model.transform(ds)

        output = output.withColumn("date", date_format(col("min_date"), "yyyyMM").cast("int"))

        output = output.select(
            col("date_block_num"),
            col("date"),
            col("shop_id"),
            col("item_id"),
            col("features"),
            col("item_cnt_month")
        )

        output
    }

    def fit(ds: Dataset[_]): FeaturesEncoder = {
        val stages = new mutable.ArrayBuffer[PipelineStage]()

        stages += new NumericalScaler()
        stages += new CategoricalEncoder()
        stages += new NLPModel()

        stages += new VectorAssembler()
            .setInputCols(features)
            .setOutputCol(outputCol)

        this.model = new Pipeline()
            .setStages(stages.toArray)
            .fit(ds)

        this
    }

}