package salespred

import salespred.tasks.FeatureEngineering

object App {

    def main(args: Array[String]) {
        println(args)

        FeatureEngineering.run(args)
    }

}