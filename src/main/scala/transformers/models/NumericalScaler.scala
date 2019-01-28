package salespred.transformers.models

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.Model

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.PipelineModel

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler

import org.apache.spark.ml.util.MLWritable
import org.apache.spark.ml.util.MLWriter

class NumericalScaler()(implicit spark: SparkSession) extends Model with MLWritable {

    val uid: String = "NumericalScaler"

    private val features = Array(
        "lat",
        "long",
        "holidays",
        "weekend_sales",
        "dayofyear",
        "weekofyear",
        "usd_rate",
        "days_range",
        "avg_item_price"
    )
    private val featuresCol = "numerical_features"
    val outputCol = "scaled_features"

    private var model: PipelineModel = _

    override def transformSchema(schema: StructType): StructType = schema
    override def copy(extra: ParamMap) = defaultCopy(extra)

    override def transform(ds: Dataset[_]): DataFrame = {
        var output = model.transform(ds)

        output = output.drop(col(featuresCol))

        for (feature <- features) {
            output = output.drop(col(feature))
        }

        output
    }

    def write: MLWriter = model.write

    def fit(ds: Dataset[_]): NumericalScaler = {
        model = new Pipeline()
        .setStages(Array(
            new VectorAssembler()
                .setInputCols(features)
                .setHandleInvalid("skip")
                .setOutputCol(featuresCol),

            new StandardScaler()
                .setInputCol(featuresCol)
                .setOutputCol(outputCol)
                .setWithStd(true)
                .setWithMean(false)
        ))
        .fit(ds.na.drop(features))

        this
    }

}