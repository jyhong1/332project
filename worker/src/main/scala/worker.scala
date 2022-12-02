package gensort.worker

import protos.network.{ResultType, ConnectionReply, SamplingReply}
import network.{NetworkClient}
import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}

import java.net.InetAddress
import java.io.{File, IOException}
import java.nio.file.{Files, Path, Paths}
import scala.concurrent.duration._
import phase.sampleMaker
import io.grpc.stub.StreamObserver
import scala.io.Source

object Worker {
  def main(args: Array[String]): Unit = {
    /*require(
      args.length >= 5 && args(0).contains(":") && args(1) == "-I" && args(
        args.length - 2
      ) == "-O"
    )*/
    val master = args(0).split(":")
    val client = NetworkClient(master(0), master(1).toInt)

    val inputPath = System.getProperty("user.dir") + "data/input"
    val sampleInputPath = System.getProperty("user.dir") + "/data/input"
    val sampleSize = 10

    try {
      val address = args.headOption.getOrElse("Team Red!")
      val connectResponse = client.connect(address)
      if (connectResponse.result == ResultType.FAILURE) {
        // TODO: if fails
      }

      // Create input files and start sampling
      // after receiving "success" connect response.
      createInputFiles(connectResponse.file)

      val samples = makeSamples(sampleInputPath, sampleSize)
      val samplingReply = client.sendSamples(samples)
      if (samplingReply.result == ResultType.FAILURE) {
        // TODO: if fails
      }

      sort("./data/received")
      client.sortPartitionComplete()

    } finally {
      client.shutdown()
    }
  }

  def createInputFiles(fileContent: String): Unit = {
    val dir = new File("./data/received")
    if (!dir.exists()) {
      dir.mkdir()
    }

    val file = new File("./data/received/receivedInput")
    file.createNewFile()

    val path = Paths.get("./data/received/receivedInput")
    Files.write(path, fileContent.getBytes())
  }

  /*Sample phase: make samples*/
  def makeSamples(inputPath: String, sampleSize: Int): Seq[String] = {
    // logger.info("[Sampling Phase] Start to make Samples")
    val samples = sampleMaker.sampling(inputPath, sampleSize)
    // logger.info("[Sample Phase] Complete to make Samples")
    samples
  }

  def comparator(s1: String, s2: String): Boolean = {
    s1.slice(0, 10) < s2.slice(0, 10)
  }

  def sortSingleFile(inputFile: File) = {
    val inputPath = inputFile.getPath()
    val inputFileSource = Source.fromFile(inputPath)
    var lines =
      for {
        line <- inputFileSource.getLines
      } yield line // type of lines = <iterator>

    val sortedList = lines.toList.sortWith((s1, s2) => comparator(s1, s2))
    val sortedString = sortedList.mkString("\n")

    // the paths of input and output file is same, which means overwritten.
    val outputFile = new File(inputPath)
    outputFile.createNewFile()
    val outputPath = Paths.get(inputPath)
    Files.write(outputPath, sortedString.getBytes())
  }

  def sort(inputDir: String) = {
    val dir = new File(inputDir)
    val files = if (dir.exists() && dir.isDirectory()) {
      dir.listFiles().filter(_.isFile()).toList
    } else {
      List[File]()
    }
    println(files)

    var lines =
      for {
        file <- files
      } yield sortSingleFile(file)
  }
}
