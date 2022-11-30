package phase

import scala.io.Source
import gensort.common.ioController


object sampleMaker {
    def sampling(inputPath: String, sampleSize: Int): Seq[String] = {
        try{
            val inputFiles = ioController.getFiles(inputPath)
            val bufferedSource = Source.fromFile(inputFiles.head)
            val samples: Seq[String] = Seq()

            for(line <- bufferedSource.getLines.take(sampleSize)){
                samples :+ line
            }
            bufferedSource.close
            samples
        } catch{
            case ex: Exception => println(ex)
        }
    }
}