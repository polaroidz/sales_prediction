package salespred

import org.apache.spark.sql.SparkSession

object Testing {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
        .master("local")
        .appName("Future Sales Prediction")
        .getOrCreate

    val filepath = "/hdfs/salespred/sales_train_v2.csv"

    val df = spark.read
        .format("csv")
        .option("header", "true")
        .option("model", "DROPMALFORMED")
        .load(filepath)
    
    df.show(10)

  }
}