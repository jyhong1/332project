//package test
import org.scalatest.funsuite._
import java.io.{File}
import phase.mergeHelper.{mergeFile, mergeFileStream}
import phase.sortHelper.sort
import phase.partitionMaker.partition
import phase.sampleMaker.makeSamples
import java.nio.file.{Files, Paths}

class MergeTest extends AnyFunSuite {
  /*test("makedir") {
    val dir = new File("./data/output/test")
    dir.mkdir()
  }*/

  /*
  test("mergestream") {
    val inputFullDirs = List("./data/testmergeinput")
    val sortDir = "./data/testmerge"
    sort(inputFullDirs, sortDir)

    val inputDirs = List("./data/testmerge")

    mergeFileStream(inputDirs)
  }*/
  /*
  test("byte") {
    val byteArray =
      Files.readAllBytes(Paths.get("./data/testmergeinput/block1"))
    print(byteArray)
  }*/
  /*
  test("partition") {
    val inputFullDirs = List("./data/testmergeinput")
    val sortDir = "./data/testmerge"
    sort(inputFullDirs, sortDir)

    partition
  }*/

  /*
  test("sample") {
    makeSamples(
      List(
        "./data/testsample/block1",
        "./data/testsample/block2",
        "./data/testsample/block3"
      ),
      "./data/output"
    )
  }
  */
}