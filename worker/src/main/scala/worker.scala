package gensort.worker

import protos.network.{
  ResultType,
  ConnectionReply,
  SamplingReply,
  ShuffleCompleteReply,
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
import phase.{sampleMaker, sortHelper, partitionMaker, mergeHelper}
import io.grpc.stub.StreamObserver
import scala.io.Source
import shufflenetwork.FileServer
import shufflenetwork.FileClient
import scala.concurrent.ExecutionContext
import java.net.InetAddress
import util.control.Breaks.{breakable, break}
import common.Utils

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

    // arguments
    val inputFilePaths = Utils.getFilePathsFromDir(inputFullDirs)
    val outputFilePath = System.getProperty("user.dir") + args.last
    val localhostIP = InetAddress.getLocalHost.getHostAddress

    val partitionsPath = System.getProperty("user.dir") + "/data/partitions"
    val sortPath = System.getProperty("user.dir") + "/data/sort"
    val shufflePath = System.getProperty("user.dir") + "/data/shuffled"

    Utils.deleteDir(partitionsPath)
    Utils.deleteDir(sortPath)
    Utils.deleteDir(shufflePath)

    try {
      /*@@@@@ connection phase @@@@@*/
      val connectResponse = client.connect(args(0))
      if (connectResponse.result == ResultType.FAILURE) {
        // TODO: if fails
      }

      /*@@@@@ sampling phase @@@@@*/
      val samples = sampleMaker.makeSamples(inputFilePaths)
      val samplingReply = client.sendSamples(samples)
      if (samplingReply.result == ResultType.FAILURE) {
        // TODO: if fails
      }
      // getid, subranges
      val id = Utils.getId(samplingReply, localhostIP)

      /*@@@@@ sort phase @@@@@*/
      val sortDir = System.getProperty("user.dir") + "/data/sort"
      sortHelper.sort(inputFullDirs, sortDir)

      /*@@@@@ partition phase @@@@@*/
      val partitionDir = System.getProperty("user.dir") + "/data/partitions"
      partitionMaker.partition(sortDir, partitionDir, samplingReply.ranges, id)

      val sortPartitionResponse = client.sortPartitionComplete()
      if (sortPartitionResponse.result == ResultType.FAILURE) {
        // TODO: if fails
      }

      /*@@@@@ shuffling phase1:shuffle ready @@@@@*/
      val workers = samplingReply.addresses
      val ranges = samplingReply.ranges
      val numWorkers = workers.length
      val shuffleDirs = System.getProperty("user.dir") + "/data/shuffled"
      val shuffleserver = FileServer(ExecutionContext.global, numWorkers - 1)
      val shuffleInputFilePaths = Utils.getFilePathsFromDir(List(partitionDir))

      shuffleserver.start()
      println("server start next")
      val result = shuffleserver.checkOnline(localhostIP, 8000)
      client.checkShuffleReady(result)

      /*@@@@@ shuffling phase2:shuffle files @@@@@*/
      var isShuffleComplete = false
      for (i <- 0 to workers.length - 1) {
        breakable {
          if (i == id) {
            val shuffleclient = FileClient(workers(i).ip, 8000)
            shuffleclient.sendPartition(
                (i + 1).toString(),
                shuffleInputFilePaths,
                shuffleDirs
            )
            shuffleclient.shutdown()
            if (i == workers.length - 1) {
              isShuffleComplete = true
              break
            }
            
          } else {
            val shuffleclient = FileClient(workers(i).ip, 8000)
            try {
              shuffleclient.sendPartition(
                (i + 1).toString(),
                shuffleInputFilePaths,
                shuffleDirs
              )
              shuffleclient.shutdown()
              if (i == workers.length - 1) {
                isShuffleComplete = true
              }
            } catch {
              case e: Exception => println(e)
            } finally {
              shuffleclient.shutdown()
            }
          }
        }
      }
      /*@@@@@ shuffling phase3:shuffle Complete @@@@@*/
      val shuffleCompleteness = client.checkShuffleComplete(isShuffleComplete)
      if (shuffleCompleteness.result == ResultType.FAILURE) {
        // TODO: if fails
      }
      shuffleserver.stop()
      /*@@@@@ merge phase @@@@@*/
      mergeHelper.mergeFileStream(List(shuffleDirs))
      client.mergeComplete()
    } catch {
      case e: Exception => println(e)
    } finally {
      client.shutdown()
    }
  }
}
