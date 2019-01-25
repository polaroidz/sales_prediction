package salespred

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SparkWrapper {

    // Stopping annoying log      
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    lazy val get = SparkSession.builder
        .master("local")
        .appName("Future Sales Prediction")
        .getOrCreate

}