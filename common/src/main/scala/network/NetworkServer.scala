package network

import protos.network.{
  NetworkGrpc,
  Address,
  Range,
  ConnectionRequest,
  ConnectionReply,
  MergeRequest,
  MergeReply,
  ShuffleReadyRequest,
  ShuffleReadyReply,
  ShuffleCompleteRequest,
  ShuffleCompleteReply,
  SortPartitionReply,
  SortPartitionRequest,
  SamplingReply,
  SamplingRequest,
  ResultType
}
import java.util.logging.Logger
import scala.concurrent.{ExecutionContext, Future}
import io.grpc.{Server, ServerBuilder, Status}
import io.grpc.stub.StreamObserver;
import java.net.InetAddress
import java.net._
import java.io.{OutputStream, FileOutputStream, File}
import scala.sys.process._
import scala.io.Source
import com.google.protobuf.ByteString
import rangegenerator.keyRangeGenerator
import shufflenetwork.FileServer
import common.{WorkerState, WorkerInfo}
import scala.concurrent.duration.Duration

object NetworkServer {
  private val logger =
    Logger.getLogger(classOf[NetworkServer].getName)

  private val port = 50051

  def apply(
      executionContext: ExecutionContext,
      numClients: Int
  ): NetworkServer = {
    new NetworkServer(executionContext, numClients)
  }
}

class NetworkServer(executionContext: ExecutionContext, numClients: Int) {
  self =>
  private[this] var server: Server = null
  private[this] var clientMap: Map[Int, WorkerInfo] = Map()
  private[this] var addressList: Seq[Address] = Seq()

  private val localhostIP = InetAddress.getLocalHost.getHostAddress

  def start(): Unit = {
    println(
      localhostIP + ":" + NetworkServer.port
    )
    server = ServerBuilder
      .forPort(NetworkServer.port)
      .addService(NetworkGrpc.bindService(new NetworkImpl, executionContext))
      .build
      .start
    NetworkServer.logger.info(
      "Server started, listening on " + NetworkServer.port
    )
    sys.addShutdownHook {
      System.err.println(
        "*** shutting down gRPC server since JVM is shutting down"
      )
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  /* *** Master's functions *** */

  /// Read input file and convert to String, Return input data into type String
  def getLine(): String = {
    val inputFile = Source.fromFile("./data/input/input1")
    var lines =
      for {
        line <- inputFile.getLines
      } yield line

    var copyInput = lines.mkString("\n")
    assert(!copyInput.isEmpty())
    copyInput
  }

  private class NetworkImpl extends NetworkGrpc.Network {
    override def connection(req: ConnectionRequest) = {
      val addr = req.addr match {
        case Some(addr) => addr
        case None       => Address(ip = "", port = 1) // TODO: error handling
      }

      NetworkServer.logger.info(
        "[Connection] Request from " + addr.ip + ":" + addr.port + " arrived"
      )

      clientMap.synchronized {
        val workerInfo = new WorkerInfo(addr.ip, addr.port)
        clientMap = clientMap + (clientMap.size + 1 -> workerInfo)
        println(clientMap)
      }

      // Waits until all of the workers are connected
      if (waitWhile(() => clientMap.size < numClients, 100000)) {
        val reply = ConnectionReply(
          result = ResultType.SUCCESS,
          message = "Connection complete to master from " + addr.ip
        )

        NetworkServer.logger.info(
          "[Connection] Input file reply to " + addr.ip + ":" + addr.port + " completed"
        )

        Future.successful(reply)
      } else {
        val reply = ConnectionReply(
          result = ResultType.FAILURE,
          message = "Connection failure to master from " + addr.ip
        )

        NetworkServer.logger.info(
          "[Connection] Input file reply to " + addr.ip + ":" + addr.port + " completed"
        )

        Future.successful(reply)
      }
    }

    override def sampling(req: SamplingRequest) = {
      val addr = req.addr match {
        case Some(addr) => addr
        case None       => Address(ip = "", port = 1) // TODO: error handling
      }
      NetworkServer.logger.info(
        "[Sampling] Sampling Request from " + addr.ip + ":" + addr.port + " arrived\n" +
          "[Sampling] test log about sampling " + req.samples.head + " arrived"
      )

      var samples: Seq[String] = Seq()

      samples.synchronized {
        samples = samples ++ req.samples
      }

      // Makes clientMap of workers
      clientMap.synchronized {
        for (i <- 1 to clientMap.size) {
          val workerInfo = clientMap(i)
          if (workerInfo.ip == addr.ip && workerInfo.port == addr.port) {
            val newWorkerInfo = new WorkerInfo(addr.ip, addr.port)
            newWorkerInfo.setWorkerState(state = WorkerState.Sampling)
            clientMap = clientMap + (i -> newWorkerInfo)
          }
        }
      }

      // TODO: needs refactoring --> just make the last worker do this != -> ==, remove synchronized
      // Makes the list of address, which will be sent to the worker
      addressList.synchronized {
        if (addressList.size != numClients) {
          for (i <- 1 to clientMap.size) {
            val workerInfo = clientMap(i)
            val address = Address(ip = workerInfo.ip, port = workerInfo.port)
            addressList = addressList :+ address
          }
        }
      }

      if (
        waitWhile(() => !isAllWorkersSameState(WorkerState.Sampling), 100000)
      ) {
        val keyRanges: Seq[Range] =
          new keyRangeGenerator(req.samples, numClients)
            .generateKeyrange()
        println(keyRanges) // remark

        val reply = SamplingReply(
          result = ResultType.SUCCESS,
          // message = "Connection complete to master from " + addr.ip
          ranges = keyRanges,
          addresses = addressList
        )

        NetworkServer.logger.info(
          "[Sampling] sampling completed from  " + addr.ip + ":" + addr.port
        )
        Future.successful(reply)
      } else {
        val reply = SamplingReply(
          result = ResultType.FAILURE
          // message = "Connection failure to master from " + addr.ip
        )

        NetworkServer.logger.info(
          "[Sampling] sampling failed from " + addr.ip + ":" + addr.port
        )
        Future.successful(reply)
      }

    }

    override def sortPartition(req: SortPartitionRequest) = {
      val addr = req.addr match {
        case Some(addr) => addr
        case None       => Address(ip = "", port = 1) // TODO: error handling
      }

      clientMap.synchronized {
        for (i <- 1 to clientMap.size) {
          val workerInfo = clientMap(i)
          if (workerInfo.ip == addr.ip && workerInfo.port == addr.port) {
            val newWorkerInfo = new WorkerInfo(addr.ip, addr.port)
            newWorkerInfo.setWorkerState(state = WorkerState.SortPartition)
            clientMap = clientMap + (i -> newWorkerInfo)
          }
        }
      }

      if (
        waitWhile(
          () => !isAllWorkersSameState(WorkerState.SortPartition),
          100000
        )
      ) {
        NetworkServer.logger.info(
          "[Sort/Partition] sort/partition completed from " + addr.ip + ":" + addr.port
        )

        val reply = SortPartitionReply(
          message = "Please start to transfer partitions"
        )

        Future.successful(reply)
      } else {
        NetworkServer.logger.info(
          "[Sort/Partition] sort/partition failed from " + addr.ip + ":" + addr.port
        )

        val reply = SortPartitionReply(
          message = "Please start to transfer partitions"
        )

        Future.successful(reply)
      }

    }

    override def shuffleReady(req: ShuffleReadyRequest) = {
      val addr = req.addr match {
        case Some(addr) => addr
        case None       => Address(ip = "", port = 1) // TODO: error handling
      }
      NetworkServer.logger.info(
        "[Shuffle Ready] File Server open Request from " + addr.ip + ":" + addr.port + " arrived"
      )

      var totalServerState: Int = 0
      totalServerState.synchronized {
        if (req.serverstate == true) {
          totalServerState += 1
        }
      }

      if (waitWhile(() => totalServerState < numClients, 100000)) {
        val reply = ShuffleReadyReply(
          result = ResultType.SUCCESS
        )

        NetworkServer.logger.info(
          "[Shuffle Ready] All shuffle servers are ready to shuffle "
        )
        Future.successful(reply)
      } else {
        val reply = ShuffleReadyReply(
          result = ResultType.FAILURE
        )
        NetworkServer.logger.info(
          "[Shuffle Ready] shuffle server at" + addr.ip + "is not opened yet."
        )
        Future.successful(reply)
      }
    }

    override def shuffleComplete(req: ShuffleCompleteRequest) = {
      val addr = req.addr match {
        case Some(addr) => addr
        case None       => Address(ip = "", port = 1) // TODO: error handling
      }
      NetworkServer.logger.info(
        "[Shuffle Complete] Worker " + addr.ip + ":" + addr.port + " completed send partitions"
      )

      var shuffleCompleteWorkers: Int = 0
      shuffleCompleteWorkers.synchronized {
        if (req.shufflecomplete == true) {
          shuffleCompleteWorkers += 1
        }
      }

      if (waitWhile(() => shuffleCompleteWorkers < numClients, 100000)) {
        val reply = ShuffleCompleteReply(
          result = ResultType.SUCCESS
        )

        NetworkServer.logger.info(
          "[Shuffle Complete] Completed re arrange every items. "
        )
        Future.successful(reply)
      } else {
        val reply = ShuffleCompleteReply(
          result = ResultType.FAILURE
        )
        NetworkServer.logger.info(
          "[Shuffle Complete] shuffle server at" + addr.ip + "is not completed yet."
        )
        Future.successful(reply)
      }
    }

    override def merge(req: MergeRequest) = {
      val addr = req.addr match {
        case Some(addr) => addr
        case None       => Address(ip = "", port = 1) // TODO: error handling
      }
      NetworkServer.logger.info(
        "[Merge] Worker " + addr.ip + ":" + addr.port + " completed merge"
      )

      var mergeCompleteWorkers: Int = 0
      mergeCompleteWorkers.synchronized {
        mergeCompleteWorkers += 1
      }

      if (waitWhile(() => mergeCompleteWorkers < numClients, 100000)) {
        val reply = MergeReply(
          result = ResultType.SUCCESS
        )

        NetworkServer.logger.info(
          "[Merge] Completed re arrange every items. "
        )

        server.shutdown()
        Future.successful(reply)
      } else {
        val reply = MergeReply(
          result = ResultType.FAILURE
        )
        NetworkServer.logger.info(
          "[Merge] shuffle server at" + addr.ip + "is not completed yet."
        )
        Future.successful(reply)
      }
    }

  }

  def waitWhile(condition: () => Boolean, timeout: Int): Boolean = {
    for (i <- 1 to timeout / 50)
      if (!condition()) return true else Thread.sleep(50)

    false
  }

  def isAllWorkersSameState(state: WorkerState): Boolean = {
    var res = true
    for (i <- 1 to clientMap.size)
      if (clientMap(i).workerState != state) res = false

    res
  }
}
