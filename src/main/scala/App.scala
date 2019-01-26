package salespred

import salespred.tasks.SaveAggregatedDataset
import salespred.readers.UtilsReader

object App {

    def main(args: Array[String]) {
        //println(args)
        //UtilsReader.shopCityData.show(10)

        SaveAggregatedDataset.run(args)
    }

}