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

import org.apache.spark.ml.util.MLWritable
import org.apache.spark.ml.util.MLWriter

import scala.collection.mutable

class FeaturesEncoder()(implicit spark: SparkSession) extends Model with MLWritable {

    val uid: String = "FeaturesEncoder"

    private val features = Array(
        "scaled_features",
        "categorical_features",
        "nlp_features"
    )
    val outputCol = "features"

    private val modelPath = s"/hdfs/salespred/models/${uid}"
    private var model: PipelineModel = _

    override def transformSchema(schema: StructType): StructType = schema
    override def copy(extra: ParamMap) = defaultCopy(extra)

    override def transform(ds: Dataset[_]): DataFrame = {
        var output = model.transform(ds)

        for (feature <- features) {
            output = output.drop(col(feature))
        }

        output
    }

    def write: MLWriter = model.write

    def fit(ds: Dataset[_]): FeaturesEncoder = {
        val stages = new mutable.ArrayBuffer[PipelineStage]()

        stages += new NumericalScaler()
        stages += new CategoricalEncoder()
        stages += new NLPModel()

        model = new Pipeline()
            .setStages(stages.toArray)
            .fit(ds)

        write.overwrite.save(modelPath)

        this
    }

}