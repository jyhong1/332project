package network

import protos.network.{
  NetworkGrpc,
  Address,
  Range,
  ResultType,
  ConnectionRequest,
  ConnectionReply,
  SamplingReply,
  SamplingRequest,
  ShuffleReadyRequest,
  ShuffleReadyReply,
  ShuffleCompleteRequest,
  ShuffleCompleteReply,
  SortPartitionReply,
  SortPartitionRequest,
  MergeRequest,
  MergeReply,
  RangeRequest,
  RangeReply
}
import java.util.logging.Logger
import scala.concurrent.{ExecutionContext, Future, Await}
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
import scala.util.{Success, Failure}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
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
  private[this] var samples: Seq[ByteString] = Seq()
  private[this] var keyranges: Seq[Range] = Seq()

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
  private class NetworkImpl extends NetworkGrpc.Network {
    override def connection(req: ConnectionRequest): Future[ConnectionReply] = {
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

    override def sampling(
        replyObserver: StreamObserver[SamplingReply]
    ): StreamObserver[SamplingRequest] = {
      NetworkServer.logger.info("[sample]: Worker tries to send sample")

      new StreamObserver[SamplingRequest] {
        var workerip: String = ""
        var workerport: Int = -1

        override def onNext(request: SamplingRequest): Unit = {
          workerip = request.addr.get.ip
          workerport = request.addr.get.port
          samples = samples :+ request.sample
        }
        override def onError(t: Throwable): Unit = {
          NetworkServer.logger.warning("[sample]: Worker failed to send sample")
          throw t
        }
        override def onCompleted(): Unit = {
          NetworkServer.logger.info("[sample]: Worker done sending sample")
          replyObserver.onNext(new SamplingReply(ResultType.SUCCESS))
          replyObserver.onCompleted

          clientMap.synchronized {
            for (i <- 1 to clientMap.size) {
              val workerInfo = clientMap(i)
              if (workerInfo.ip == workerip && workerInfo.port == workerport) {
                val newWorkerInfo = new WorkerInfo(workerip, workerport)
                newWorkerInfo.setWorkerState(state = WorkerState.Sampling)
                clientMap = clientMap + (i -> newWorkerInfo)
              }
            }
          }
          addressList.synchronized {
            if (addressList.size != numClients) {
              for (i <- 1 to clientMap.size) {
                val workerInfo = clientMap(i)
                val address =
                  Address(ip = workerInfo.ip, port = workerInfo.port)
                addressList = addressList :+ address
              }
            }
          }
        }
      }
    }

    override def range(req: RangeRequest): Future[RangeReply] = {
      val addr = req.addr match {
        case Some(addr) => addr
        case None       => Address(ip = "", port = 1)
      }
      keyranges.synchronized {
        val keyRangesFuture = new keyRangeGenerator(samples, numClients)
          .generateKeyrange()
        keyranges = Await.result(keyRangesFuture, Duration.Inf)
      }

      clientMap.synchronized {
        for (i <- 1 to clientMap.size) {
          val workerInfo = clientMap(i)
          if (workerInfo.ip == addr.ip && workerInfo.port == addr.port) {
            val newWorkerInfo = new WorkerInfo(addr.ip, addr.port)
            newWorkerInfo.setWorkerState(state = WorkerState.Range)
            clientMap = clientMap + (i -> newWorkerInfo)
          }
        }
      }

      if (
        waitWhile(() => !isAllWorkersSameState(WorkerState.Range), 100000000)
      ) {
        NetworkServer.logger.info("[Range] Try to broadcast range ")
        val reply = RangeReply(ResultType.SUCCESS, keyranges, addressList)
        Future.successful(reply)
      } else {
        NetworkServer.logger.warning("[Range] range failed")
        Future.failed(new Exception)
      }
    }

    override def sortPartition(req: SortPartitionRequest) = {
      val addr = req.addr match {
        case Some(addr) => addr
        case None       => Address(ip = "", port = 1)
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
          100000000
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
        case None       => Address(ip = "", port = 1)
      }
      NetworkServer.logger.info(
        "[Shuffle Ready] File Server open Request from " + addr.ip + ":" + addr.port + " arrived"
      )

      clientMap.synchronized {
        for (i <- 1 to clientMap.size) {
          val workerInfo = clientMap(i)
          if (workerInfo.ip == addr.ip && workerInfo.port == addr.port) {
            val newWorkerInfo = new WorkerInfo(addr.ip, addr.port)
            newWorkerInfo.setWorkerState(state = WorkerState.ShuffleReady)
            clientMap = clientMap + (i -> newWorkerInfo)
          }
        }
      }

      if (
        waitWhile(
          () => !isAllWorkersSameState(WorkerState.ShuffleReady),
          100000000
        )
      ) {
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
          "[Shuffle Ready] shuffle server at" + addr.ip + "is not opend yet."
        )
        Future.successful(reply)
      }
    }

    override def shuffleComplete(req: ShuffleCompleteRequest) = {
      val addr = req.addr match {
        case Some(addr) => addr
        case None       => Address(ip = "", port = 1)
      }
      NetworkServer.logger.info(
        "[Shuffle Complete] Worker " + addr.ip + ":" + addr.port + " completed send partitions"
      )

      clientMap.synchronized {
        for (i <- 1 to clientMap.size) {
          val workerInfo = clientMap(i)
          if (workerInfo.ip == addr.ip && workerInfo.port == addr.port) {
            val newWorkerInfo = new WorkerInfo(addr.ip, addr.port)
            newWorkerInfo.setWorkerState(state = WorkerState.ShuffleComplete)
            clientMap = clientMap + (i -> newWorkerInfo)
          }
        }
      }

      if (
        waitWhile(
          () => !isAllWorkersSameState(WorkerState.ShuffleComplete),
          100000000
        )
      ) {
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
        case None       => Address(ip = "", port = 1)
      }
      NetworkServer.logger.info(
        "[Merge] Worker " + addr.ip + ":" + addr.port + " completed merge"
      )

      var mergeCompleteWorkers: Int = 0
      mergeCompleteWorkers.synchronized {
        mergeCompleteWorkers += 1
      }
      if (waitWhile(() => mergeCompleteWorkers < numClients, 100000000)) {
        val reply = MergeReply(
          result = ResultType.SUCCESS
        )
        NetworkServer.logger.info(
          "[Merge] Completed re arrange every items. "
        )

        addressList.synchronized {
          if (addressList(1) == Address(addr.ip, addr.port)) {
            for (i <- 1 to addressList.size) {
              print(addressList(i).ip + ":" + addressList(i).port)
              if (i != addressList.size) print(", ")
            }
          }
        }
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
    /*
    def getkeyranges(): Unit = {
      if (isAllWorkersSameState(WorkerState.Sampling)) {
        val f = Future {
          clientMap.synchronized {
            for (i <- 1 to clientMap.size) {
              val workerInfo = clientMap(i)
              if (workerInfo.ip == workerip && workerInfo.port == workerport) {
                if (i == 1) {
                  keyranges = new keyRangeGenerator(samples, numClients)
                    .generateKeyrange()
                }
                val newWorkerInfo = new WorkerInfo(workerip, workerport)
                newWorkerInfo.setWorkerState(state = WorkerState.Range)
                clientMap = clientMap + (i -> newWorkerInfo)
              }
            }
          }
        }
        f.onComplete {
          case Success(_) => {
            NetworkServer.logger.info("[tryPivot] Pivot done successfully")
          }
          case Failure(t) => {
            NetworkServer.logger.info(
              "[tryPivot] Pivot failed: " + t.getMessage
            )
          }
        }
      }
    }*/
  }
}
