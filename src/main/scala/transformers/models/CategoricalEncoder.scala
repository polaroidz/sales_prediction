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

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.OneHotEncoderEstimator

import scala.collection.mutable

class CategoricalEncoder()(implicit spark: SparkSession) extends Model {

    val uid: String = "CategoricalEncoder"

    private val features = Array(
        "shop_type",
        "city_name",
        "item_category_name"
    )
    private val featuresIdx = features.map(e => s"${e}_idx")
    private val featuresOh = features.map(e => s"${e}_oh")
    val outputCol = "categorical_features"

    private val modelPath = s"/hdfs/salespred/models/${uid}"

    override def transformSchema(schema: StructType): StructType = schema
        .add(outputCol, VectorType)

    override def copy(extra: ParamMap) = defaultCopy(extra)

    override def transform(ds: Dataset[_]): DataFrame = {
        var tds = ds

        for (feature <- features) {
            tds = tds.withColumn(feature, 
                when(col(feature).isNull, lit("NA")).otherwise(col(feature)))
        }

        val loadedModel = PipelineModel.read.load(modelPath)
        var output = loadedModel.transform(tds)

        for (i <- Array.range(0, features.size)) {
            output = output.drop(features(i))
            output = output.drop(featuresIdx(i))
            output = output.drop(featuresOh(i))
        }

        output
    }

    def fit(ds: Dataset[_]): CategoricalEncoder = {
        var tds = ds

        for (feature <- features) {
            tds = tds.withColumn(feature, 
                when(col(feature).isNull, lit("NA")).otherwise(col(feature)))
        }

        val stages = new mutable.ArrayBuffer[PipelineStage]()

        for (i <- Array.range(0, features.size)) {
            val inFeature = features(i)
            val outFeature = featuresIdx(i)

            stages += new StringIndexer()
                .setInputCol(inFeature)
                .setOutputCol(outFeature)
        }

        stages += new OneHotEncoderEstimator()
            .setInputCols(featuresIdx)
            .setOutputCols(featuresOh)
            
        stages += new VectorAssembler()
            .setInputCols(featuresOh)
            .setOutputCol(outputCol)

        val trainedModel = new Pipeline()
            .setStages(stages.toArray)
            .fit(tds)

        trainedModel.write.overwrite.save(modelPath)

        this
    }

}