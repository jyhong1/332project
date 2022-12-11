package phase

import common.Utils
import java.nio.file.{Files, Path, Paths}
import java.io.{File, FileWriter, BufferedWriter}
import Utils.getFilePathsFromDir
import scala.io.Source
import scala.collection.mutable.ListBuffer

object mergeHelper {
  def mergeFile(
      inputDirs: String,
      outputFilePath: String,
      id: Int,
      numSubrange: Int
  ) = {
    Utils.createdir(outputFilePath)

    /*read all subrange directories*/
    var inputDirLists: Seq[String] = Seq()
    for (i <- 1 to numSubrange) {
      val inputDir = inputDirs + "/subrange" + i.toString
      inputDirLists = inputDirLists :+ inputDir
    }

    /*merge files into one in same subrange*/
    for (d <- inputDirLists) {
      val files = Utils.getFilePathsFromDir(List(d))
      var partitions: Seq[Seq[String]] = Seq()
      for (f <- files) {
        var items = Utils.getFile(f)
        partitions :+ items
      }

      /*make partition file names*/
      var number = +1
      var filenames: Seq[String] = Seq()
      for (i <- 1 to numSubrange) {
        val filename =
          outputFilePath + "/partition" + ((id - 1) * numSubrange).toString
        filenames = filenames :+ filename
      }

      for (f <- filenames) {
        val writer = new BufferedWriter(new java.io.FileWriter(f))
        for (line <- mergeSort(partitions)) {
          writer.write(line)
        }
        writer.close()
      }
    }
  }

  def mergeSort(allpartitions: Seq[Seq[String]]): Seq[String] =
    allpartitions match {
      case Nil       => Nil
      case xs :: Nil => xs
      case _ =>
        val (left, right) = allpartitions.splitAt(allpartitions.length / 2)
        merge(mergeSort(left), mergeSort(right))
    }
  def merge(seq1: Seq[String], seq2: Seq[String]): Seq[String] =
    (seq1, seq2) match {
      case (Nil, _) => seq2
      case (_, Nil) => seq1
      case (x :: xs, y :: ys) =>
        if ((Utils.comparator(x, y))) x +: merge(xs, seq2)
        else y +: merge(seq1, ys)
    }

  def getMinKeyIndex(lines: List[String]) = {
    var min = "~~~~~~~~~~"
    var idx = 0

    for (i <- 0 to lines.size - 1) {
      val key = lines(i).take(10)
      if (key < min) {
        min = key
        idx = i
      }
    }

    idx
  }

  def isIteratorEnd(iter: List[Iterator[String]]) = {
    var res = true
    for (i <- 0 to iter.size - 1) {
      if (iter(i).hasNext) res = false
    }

    res
  }

  /* Merge phase function start*/
  def mergeFile(inputDirs: List[String], outputFilePath: String) = {
    val filePaths = getFilePathsFromDir(inputDirs.toList)
    val files = filePaths.map(filePath =>
      Source.fromFile(filePath).getLines()
    ) // file first line iterator

    println(files)

    val filesCopy = new ListBuffer[Iterator[String]]()
    var outputBuffer = new ListBuffer[String]()
    val outputBufferMaxSize = 1000000 // 100MB

    var outputFileNum = 1
    while (!isIteratorEnd(files)) {
      while (outputBuffer.size <= outputBufferMaxSize) {
        // var filesCopy = new ListBuffer[Iterator[String]]()
        filesCopy.clear()
        files.foreach(iter => filesCopy += iter)

        val heads = filesCopy.map(iter => iter.next()).toList

        val minIdx = getMinKeyIndex(heads)
        val min = files(minIdx).next()
        outputBuffer += min
      }

      println(outputBuffer)
      val outputString = outputBuffer.mkString("\n")
      val outputDirPath = "./data/output"

      val dir = new File(outputDirPath)
      if (!dir.exists()) {
        dir.mkdir()
      }

      val outputFile = new File(
        outputDirPath + "/partition_" + outputFileNum
      )
      outputFileNum += 1

      outputFile.createNewFile()
      val path = Paths.get(outputFile.getPath())
      Files.write(path, outputString.getBytes())

    }

    /*
    def pqOrder(line: String) = line.take(10)

    val pq = PriorityQueue()(Ordering.by(pqOrder))
    val pqMaxSize = 1000000 // 100MB

    val a = Seq("a", "b")
    while (pq.size <= pqMaxSize) {
      pq.enqueue("a", "b")
    }*/

    /* Merge phase function start*/

  }

  def isStreamEmpty(stream: List[Stream[String]]) = {
    var res = true
    for (i <- 0 to stream.size - 1) {
      if (!stream(i).isEmpty) res = false
    }

    res
  }

  def mergeFileStream(inputDirs: List[String]) = {
    val filePaths = getFilePathsFromDir(inputDirs.toList)
    var files = filePaths
      .map(filePath => Source.fromFile(filePath).getLines().toStream)

    var outputBuffer = new ListBuffer[String]()
    val outputBufferMaxSize = 200000 // 20MB

    var outputFileNum = 1
    while (!isStreamEmpty(files.toList)) {
      while (
        outputBuffer.size <= outputBufferMaxSize && !isStreamEmpty(files.toList)
      ) {
        val minList = new ListBuffer[String]()
        files.foreach(file => {
          if (!file.isEmpty) minList += file.head
          else minList += "~~~~~~~~~~"
        })
        val minIdx = getMinKeyIndex(minList.toList)
        val min = files(minIdx).head
        outputBuffer += min

        var newFiles = List[Stream[String]]()
        for (i <- 0 to files.size - 1) {
          if (i == minIdx) {
            newFiles = newFiles :+ files(minIdx).drop(1)
          } else {
            newFiles = newFiles :+ files(i)
          }
        }
        files = newFiles
      }

      val outputString = outputBuffer.mkString("\n")
      val outputDirPath = "./data/output"

      val dir = new File(outputDirPath)
      if (!dir.exists()) {
        dir.mkdir()
      }

      val outputFile = new File(
        outputDirPath + "/partition_" + outputFileNum
      )
      outputFileNum += 1

      outputFile.createNewFile()
      val path = Paths.get(outputFile.getPath())
      Files.write(path, outputString.getBytes())
      outputBuffer.clear()
    }

  }
}