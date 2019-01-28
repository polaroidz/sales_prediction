package salespred

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import salespred.utils.FileUtils

import salespred.tasks.MakingPredictions
import salespred.tasks.FeatureEngineering

object App {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    implicit val spark = SparkSession.builder
        .master("local")
        .appName("Future Sales Prediction")
        .getOrCreate

    implicit val files = new FileUtils()

    def main(args: Array[String]) {
        //val task = new FeatureEngineering()
        val task = new MakingPredictions()

        task.run(args)
    }

}