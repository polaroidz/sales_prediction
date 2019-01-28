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
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF

import org.apache.spark.ml.util.MLWritable
import org.apache.spark.ml.util.MLWriter

import scala.collection.mutable

class NLPModel()(implicit spark: SparkSession) extends Model with MLWritable {

    val uid: String = "NLPModel"

    private val features = Array(
        "shop_name",
        "item_name"
    )
    private val featuresToken = features.map(e => s"${e}_tkn")
    private val featuresHash = features.map(e => s"${e}_hsh")
    private val featuresIDF = features.map(e => s"${e}_idf")    
    val outputCol = "nlp_features"

    // Number of features on the TF Phase
    val numFeaturesTF = 5

    private var model: PipelineModel = _

    override def transformSchema(schema: StructType): StructType = schema
    override def copy(extra: ParamMap) = defaultCopy(extra)

    override def transform(ds: Dataset[_]): DataFrame = {
        var output = model.transform(ds)

        for (i <- Array.range(0, features.size)) {
            val feature = features(i)
            val featureToken = featuresToken(i)
            val featureHash = featuresHash(i)
            val featureIDF = featuresIDF(i)

            output = output.drop(col(feature))
            output = output.drop(col(featureToken))
            output = output.drop(col(featureHash))
            output = output.drop(col(featureIDF))
        }

        output
    }

    def write: MLWriter = model.write

    def fit(ds: Dataset[_]): NLPModel = {
        val stages = new mutable.ArrayBuffer[PipelineStage]()

        for (i <- Array.range(0, features.size)) {
            val feature = features(i)
            val featureToken = featuresToken(i)
            val featureHash = featuresHash(i)
            val featureIDF = featuresIDF(i)

            stages += new Tokenizer()
                .setInputCol(feature)
                .setOutputCol(featureToken)
            
            stages += new HashingTF()
                .setInputCol(featureToken)
                .setOutputCol(featureHash)
                .setNumFeatures(numFeaturesTF)
            
            stages += new IDF()
                .setInputCol(featureHash)
                .setOutputCol(featureIDF)
        }

        stages += new VectorAssembler()
            .setInputCols(featuresIDF)
            .setOutputCol(outputCol)

        model = new Pipeline()
            .setStages(stages.toArray)
            .fit(ds.na.drop(features))

        this
    }

}