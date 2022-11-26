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
import io.grpc.{Server, ServerBuilder,Status}
import io.grpc.stub.StreamObserver
import java.net.InetAddress

object MasterWorkerServer {
  private val logger =
    Logger.getLogger(classOf[MasterWorkerServer].getName)

  def main(args: Array[String]): Unit = {
    // should be args len 1 which is master 3(num of workers)
    require(args.length == 1 && args(0).toInt >= 1 && args(0).toInt <= 10)
    val num_workers = args(0)

    val server = new MasterWorkerServer(ExecutionContext.global)
    server.start()
    server.blockUntilShutdown()
  }

  private val port = 50051
}

class MasterWorkerServer(executionContext: ExecutionContext) { self =>
  private[this] var server: Server = null

  private def start(): Unit = {
    println(
      InetAddress.getLocalHost.getHostAddress + ":" + MasterWorkerServer.port
    )
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
      MasterWorkerServer.logger.info(
        "[Connection] Request from " + req.ip + ":" + req.port + " arrived"
      )
      val reply = ConnectionReply(
        result = ResultType.SUCCESS,
        message = "Connection complete from " + req.ip
      )
      Future.successful(reply)
    }

    override def sampling(req: StreamObserver[SamplingRequest]): StreamObserver[SamplingRequest] = {
      logger.info("[Sample] Workers tries to send samples")
      new StreamObserver[SamplingRequest]{
        var workerId: Int = -1
        var samples: List[String] = List()

        override def onNext(req: SamplingRequest): Unit ={
          workerId = request.id
          samples = request.samples
        }

        override def onError(t:Throwable): Unit = {
          logger.warning(s"[Sample]: Worker $workerId failed to send samples: ${Status.fromThrowable(t)}")
            throw t
        }

        override def onCompleted(): Unit = {
          logger.info(s"[Sample]: Worker $workerId done sending samples")

          replyingObserver.onNext(new SamplingReply(result = ResultType.SUCCESS))
          replyingObserver.onCompleted

        /*synchronization implementation required!*/

        }
      }
    }

    override def sortPartition(req: SortPartitionRequest) = {
      val reply = SortPartitionReply(
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
    
    override def merge(req: MergeRequest) = {
      val reply = MergeReply(
        message = "Connection complete from " + req.name
      )
      Future.successful(reply)
    }
    
  }

}
