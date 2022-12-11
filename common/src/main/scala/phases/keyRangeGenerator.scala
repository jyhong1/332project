package rangegenerator

import scala.collection.mutable.Buffer
import protos.network.Range
import common.Utils
import com.google.protobuf.ByteString
import scala.concurrent._

class keyRangeGenerator(
    allSamples: scala.collection.Seq[ByteString],
    numWorkers: Int
) {
  val outputSize = 1000000 // 100MB
  val temp = allSamples.map(x => x.toStringUtf8)
  val sortedSamples = temp.sortWith((s1, s2) => Utils.comparator(s1, s2))

  def generateKeyrange(): Future[Seq[Range]] = {
    val p = Promise[Seq[Range]]

    val numPoints = numWorkers - 1
    val term = sortedSamples.length / numWorkers
    val remain = sortedSamples.length % numWorkers

    var points: Buffer[Int] = Buffer()
    for (i <- 0 to remain - 1) {
      points = points :+ ((term + 1) * (i + 1))
    }
    for (i <- remain * (term + 1) + term to sortedSamples.length - 1 by term) {
      points = points :+ i
    }
    var ranges: Buffer[Range] = Buffer()
    for (i <- 0 to points.length - 1) {
      if (i == 0) {
        var el = Range("          ", sortedSamples(points.head))
        ranges = ranges :+ el
      } else {
        var el = Range(sortedSamples(points(i - 1)), sortedSamples(points(i)))
        ranges = ranges :+ el
      }
    }
    ranges = ranges :+ Range(sortedSamples(points.last), "~~~~~~~~~~")

    p.success(ranges)
    p.future
  }
}
