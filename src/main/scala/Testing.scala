package salespred

import org.apache.spark.{SparkContext, SparkConf}

object Testing {
  def main(args: Array[String]) {
    val conf = new SparkConf()
        .setAppName("SparkMe Application")
        .setMaster("local")

    val sc = new SparkContext(conf)

    val fileName = "/hdfs/salespred/sales_train_v2.csv"
    val lines = sc.textFile(fileName).cache

    val c = lines.count
    println(s"There are $c lines in $fileName")
  }
}