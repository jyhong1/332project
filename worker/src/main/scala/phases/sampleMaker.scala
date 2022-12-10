package phase

import scala.io.Source
import common.Utils

/*
  sampleMaker helps to make samples and return samples to worker.
  worker makes request to Master with samples made by sampleMaker
  we can adjust sample ratio with sampleRatio
*/
object sampleMaker {
  def sampling(inputFilePath: String): Seq[String] = {
    val bufferedSource = Utils.getFile(inputFilePath)
    val sampleRatio = 50.0
    var numItems = bufferedSource.length
    var numSamples:Int = 0

    //println("bufferlength", bufferedSource.length)
    //println("inputFilePath",inputFilePath)

    if((sampleRatio / 100.0).toFloat * numItems.toFloat < 1.0){
      numSamples = numSamples  + 1
    } else {
      numSamples = ((sampleRatio / 100).toFloat * numItems).toInt 
    }
    //println("numSamples",numSamples.toString())

    val (samples,remains) = bufferedSource.splitAt(numSamples)
    val result = samples.map(_.slice(0, 10)).toSeq
    //println("result\n")
    result.foreach(println)

    result
  }

  def makeSamples(inputPaths: List[String]): Seq[String] = {
    val samples =
      inputPaths.flatMap(path => sampling(path)).toSeq 
    
    //println("inputPaths\n")
    //inputPaths.foreach(println)
    //println("samples\n")
    //samples.foreach(println)

    samples
  }
}
