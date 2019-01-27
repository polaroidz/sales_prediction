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

import scala.collection.mutable

class FeaturesEncoder()(implicit spark: SparkSession) extends Model {

    val uid: String = "FeaturesEncoder"

    private val features = Array(
        "scaled_features",
        "categorical_features",
        "nlp_features"
    )
    val outputCol = "features"

    private val modelPath = s"/hdfs/salespred/models/${uid}"

    override def transformSchema(schema: StructType): StructType = schema
    override def copy(extra: ParamMap) = defaultCopy(extra)

    override def transform(ds: Dataset[_]): DataFrame = {
        val loadedModel = PipelineModel.read.load(modelPath)
        var output = loadedModel.transform(ds)

        for (feature <- features) {
            output = output.drop(col(feature))
        }

        output
    }

    def fit(ds: Dataset[_]): FeaturesEncoder = {
        val stages = new mutable.ArrayBuffer[PipelineStage]()

        stages += new NumericalScaler()
        stages += new CategoricalEncoder()
        stages += new NLPModel()

        val trainedModel = new Pipeline()
            .setStages(stages.toArray)
            .fit(ds)

        trainedModel.write.overwrite.save(modelPath)

        this
    }

}