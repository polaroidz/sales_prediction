package salespred

object Testing {
    val trainingDataPath = "/hdfs/salespred/sales_train_v2.csv"

    def trainingData =
        SparkWrapper.get.read
            .format("csv")
            .option("header", "true")
            .option("model", "DROPMALFORMED")
            .load(trainingDataPath)

    def main(args: Array[String]) {
        val df = trainingData

        df.show(10)
    }
}