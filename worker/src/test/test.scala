//package test
import org.scalatest.funsuite._
import java.io.{File}
import gensort.worker.Worker.{mergeFile, mergeFileStream}

class MergeTest extends AnyFunSuite {
  /*test("makedir") {
    val dir = new File("./data/output/test")
    dir.mkdir()
  }*/

  test("mergestream") {
    val dir = new File("./data/sort")

    val inputDirs = List("./data/sort/input1", "./data/sort/input2")

    mergeFileStream(inputDirs, "")
  }
}
