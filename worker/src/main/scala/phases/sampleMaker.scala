package phase

import scala.io.Source

object sampleMaker {
  def sampling(inputFilePath: String, sampleSize: Int): Seq[String] = {
    val bufferedSource = Source.fromFile(inputFilePath)
    val samples =
      bufferedSource.getLines.take(sampleSize).map(_.slice(0, 10)).toSeq
    samples
  }
}
