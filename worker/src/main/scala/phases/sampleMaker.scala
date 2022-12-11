package phase

import common.Utils
import scala.io.Source
import java.io.{File,BufferedWriter,FileWriter}

object sampleMaker {
  def makeSamples(inputPaths: List[String], outputPath: String): Unit = {
    val samples = inputPaths.flatMap(file => sampleSingleFile(file)).toList
    
    Utils.createdir(outputPath)
    val filename = outputPath + "/samples"
    val samplewrite = new BufferedWriter(new FileWriter(filename))
    samplewrite.flush()
    for ( line <- samples){
      samplewrite.write(line.take(10)+"\n")
    }
    samplewrite.close()
  }

  def sampleSingleFile(inputPath: String):List[String] = {
    val sampleRatio = 1
    val samplesource = Source.fromFile(inputPath).getLines.toList
    val numItems = samplesource.length
    val numPickedSample = (sampleRatio / 100.0).toFloat * numItems.toFloat
    val numSamples = if (numPickedSample < 1) 1 else numPickedSample
    val (result,_) = samplesource.splitAt(numSamples.toInt)
    result
  }
}