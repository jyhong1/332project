package phase

import scala.io.Source

object sampleMaker {
  def sampling(inputPath: String, sampleSize: Int): Seq[String] = {
    val fileName = "/input1"
    val inputFile = inputPath + fileName
    val bufferedSource = Source.fromFile(inputFile)
    val samples =
      bufferedSource.getLines.take(sampleSize).map(_.slice(0, 10)).toSeq
    samples
  }
}
