package gensort.worker

import protos.network.{
  ResultType,
  ConnectionReply,
  SamplingReply,
  Range,
  Address
}
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
    require(
      args.length >= 5 && args(0).contains(":") && args(1) == "-I" && args(
        args.length - 2
      ) == "-O"
    )
    val master = args(0).split(":")
    val client = NetworkClient(master(0), master(1).toInt)

    val inputDirs = args.slice(2, args.length - 2).toList
    val inputFullDirs =
      inputDirs.map(path => System.getProperty("user.dir") + path)
    val inputFilePaths = getFilePathsFromDir(inputFullDirs)
    println(inputFilePaths)

    val sampleSize = 10 // TODO: Change this

    try {
      val connectResponse = client.connect(args(0))
      if (connectResponse.result == ResultType.FAILURE) {
        // TODO: if fails
      }

      // Create input files and start sampling
      // after receiving "success" connect response.

      val samples = makeSamples(inputFilePaths, sampleSize)
      val samplingReply = client.sendSamples(samples)
      if (samplingReply.result == ResultType.FAILURE) {
        // TODO: if fails
      }

      // ### Sort ###
      sort(inputFullDirs)

      // ### Partition ###
      val sortDir = System.getProperty("user.dir") + "/data/sort"
      val dir = new File(sortDir)

      assert(dir.exists() && dir.isDirectory())
      val sortDirs = if (dir.exists() && dir.isDirectory()) {
        dir.listFiles().filter(_.isDirectory()).map(_.getPath()).toList
      } else {
        List[String]()
      }

      assert(sortDirs.size > 0)
      partition(sortDirs, samplingReply.ranges, samplingReply.addresses)

      // ### Shuffle ###

      // ### Merge ###
      // mergeFile("./data/partition")

      client.sortPartitionComplete()

    } finally {
      client.shutdown()
    }
  }

  // *** Unused ***
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
  def makeSamples(inputPaths: List[String], sampleSize: Int): Seq[String] = {
    // logger.info("[Sampling Phase] Start to make Samples")
    val samples =
      inputPaths.flatMap(path => sampleMaker.sampling(path, sampleSize)).toSeq
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

    // TODO: this needs to be fixed...
    val inputPathSplit = inputPath.split("data")
    val outputDirPathSplit = inputPathSplit(1).split("/")
    val outputDirPathString =
      inputPathSplit(0) + "data/sort/" + outputDirPathSplit(1)

    val outputDir = new File(outputDirPathString)
    if (!outputDir.exists()) {
      outputDir.mkdir()
    }

    val outputPathString = inputPathSplit(0) + "data/sort" + inputPathSplit(1)
    val outputFile = new File(inputPath)
    outputFile.createNewFile()
    val outputPath = Paths.get(outputPathString)
    Files.write(outputPath, sortedString.getBytes())
  }

  def sort(inputDirs: List[String]) = {
    val dir = new File("./data/sort")
    if (!dir.exists()) {
      dir.mkdir()
    }

    val filePaths = getFilePathsFromDir(inputDirs)
    println(filePaths)
    val files = filePaths.map { path =>
      val file = new File(path)
      file.createNewFile()
      file
    }

    var lines =
      for {
        file <- files
      } yield sortSingleFile(file)
  }

  def partition(
      inputDirs: List[String],
      ranges: Seq[protos.network.Range],
      address: Seq[Address]
  ) = {
    assert(ranges.size == address.size)

  }

  /* Merge phase function start*/
  def mergeFile(inputDirs: String) = {
    val dir = new File(inputDirs)
    val files = if (dir.exists() && dir.isDirectory()) {
      dir.listFiles().filter(_.isFile()).toList
    } else {
      List[File]()
    }
    println(files)

    var lines =
      for {
        file <- files
      } yield getLine(file)

    val mergedString = lines.mkString("\n")

    // println("mergedString\n" + mergedString)

    var strings =
      for {
        string <- mergedString.split('\n')
      } yield string // type of lines = <iterator>

    val sortedList = strings.toList.sortWith((s1, s2) => comparator(s1, s2))
    val sortedString = sortedList.mkString("\n")

    // println("Sort mergedString\n" + sortedString)
    val makeMergeFile = new File(inputDirs + "/mergedFile")
    val path = Paths.get(inputDirs + "/mergedFile")
    Files.write(path, sortedString.getBytes())
  }

  def getLine(fileName: File): String = {
    val inputFile = Source.fromFile(fileName.getPath)
    var lines =
      for {
        line <- inputFile.getLines
      } yield line // type of lines = <iterator>

    var copyInput = lines.mkString("\n")
    assert(!copyInput.isEmpty())
    copyInput
  }

  def getFilePathsFromDir(dirs: List[String]): List[String] = {
    val filePaths = dirs.flatMap { dirPath =>
      val dir = new File(dirPath)
      val files = if (dir.exists() && dir.isDirectory()) {
        dir.listFiles().filter(_.isFile()).map(file => file.getPath()).toList
      } else {
        List[String]()
      }
      files
    }
    filePaths
  }

}
