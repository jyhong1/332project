package phase

import java.io.{File, PrintWriter}
import scala.io.Source
import gensort.common.ioController


object sampleSender {
    def sample(inputPath: String, sampleSize: Int): List[String] = {
        try{
            val inputFiles = ioController.getFiles(inputPath)
            val bufferedSource = Source.fromFile(inputFiles.head)
            val samples: List[String] = List()

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
