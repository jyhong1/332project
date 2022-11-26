package gensort.master

import protos.network.{
  NetworkGrpc,
  ConnectionRequest,
  ConnectionReply,
  MergeRequest,
  MergeReply,
  ShuffleReply,
  ShuffleRequest,
  SortPartitionReply,
  SortPartitionRequest,
  SamplingReply,
  SamplingRequest,
  ResultType
}
import java.util.logging.Logger
import scala.concurrent.{ExecutionContext, Future}
import io.grpc.{Server, ServerBuilder}
import java.net.InetAddress

object MasterWorkerServer {
  private val logger = Logger.getLogger(classOf[MasterWorkerServer].getName)

  def main(args: Array[String]): Unit = {
    // should be args len 1 which is master 3(num of workers)
    require(args.length == 1 && args(0).toInt >= 1 && args(0).toInt <= 10)
    val num_workers = args(0)

    println(InetAddress.getLocalHost.getHostAddress)
    val server = new MasterWorkerServer(ExecutionContext.global)
    server.start()
    server.blockUntilShutdown()
  }

  private val port = 50051
}

class MasterWorkerServer(executionContext: ExecutionContext) { self =>
  private[this] var server: Server = null

  private def start(): Unit = {
    server = ServerBuilder
      .forPort(MasterWorkerServer.port)
      .addService(NetworkGrpc.bindService(new NetworkImpl, executionContext))
      .build
      .start
    MasterWorkerServer.logger.info(
      "Server started, listening on " + MasterWorkerServer.port
    )
    sys.addShutdownHook {
      System.err.println(
        "*** shutting down gRPC server since JVM is shutting down"
      )
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  private def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  private class NetworkImpl extends NetworkGrpc.Network {
    override def connection(req: ConnectionRequest) = {
      println("[Connection] Request from" + req.ip + ":" + req.port + "arrived")
      val reply = ConnectionReply(
        result = ResultType.SUCCESS,
        message = "Connection complete from " + req.ip
      )
      Future.successful(reply)
    }

    override def merge(req: MergeRequest) = {
      val reply = MergeReply(
        message = "Connection complete from " + req.name
      )
      Future.successful(reply)
    }
    override def sampling(req: SamplingRequest) = {
      val reply = SamplingReply(
        message = "Connection complete from " + req.name
      )
      Future.successful(reply)
    }
    override def shuffle(req: ShuffleRequest) = {
      val reply = ShuffleReply(
        message = "Connection complete from " + req.name
      )
      Future.successful(reply)
    }
    override def sortPartition(req: SortPartitionRequest) = {
      val reply = SortPartitionReply(
        message = "Connection complete from " + req.name
      )
      Future.successful(reply)
    }
  }

}
