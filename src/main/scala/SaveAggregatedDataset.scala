package salespred

import org.apache.spark.sql.functions._

// TODO: Investigate these 0.1 prices
//df.groupBy(col("item_price")).count.orderBy(col("count").desc).show(100)
//df.filter(col("item_price") === 0.1).show(20)

object SaveAggregatedDataset {

    private val outputPath = "/hdfs/salespred/output/sales_aggregated.csv"

    def main(args: Array[String]) {
        val df = DatasetReader.loadAggregatedData

        df.write
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .save(outputPath)
    }
}