package salespred

import salespred.tasks.DatasetFormatting

object App {

    def main(args: Array[String]) {
        val task = new DatasetFormatting()

        task.run()
    }

}