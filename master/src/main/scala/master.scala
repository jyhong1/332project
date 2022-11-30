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
  FileRequest,
  FileReply,
  ResultType
}
import java.util.logging.Logger
import scala.concurrent.{ExecutionContext, Future}
import io.grpc.{Server, ServerBuilder, Status}
import java.net.InetAddress
import java.net._
import java.io.{OutputStream, FileOutputStream, File}
import scala.sys.process._
import scala.io.Source
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver;

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

  // read input file and convert to String, Return input data into type String
  private def getLine(): String = {
    val inputFile = Source.fromFile("./data/input/input1")
    var lines =
      for {
        line <- inputFile.getLines
      } yield line // type of lines = <iterator>

    var copyInput = lines.mkString("\n")
    assert(!copyInput.isEmpty())
    copyInput
  }

  private class NetworkImpl extends NetworkGrpc.Network {
    override def connection(req: ConnectionRequest) = {
      MasterWorkerServer.logger.info(
        "[Connection] Request from " + req.ip + ":" + req.port + " arrived"
      )
      val reply = ConnectionReply(
        result = ResultType.SUCCESS,
        message = "Connection complete to master from " + req.ip
      )
      Future.successful(reply)
    }

    override def merge(req: MergeRequest) = {
      val reply = MergeReply(
        message = "Connection complete from " + req.name
      )
      Future.successful(reply)
    }
    override def sampling(req: SamplingRequest)= {
      val reply = SamplingReply(
        result = ResultType.SUCCESS
      )
      MasterWorkerServer.logger.info(
        "[Sampling phase] sampling done with"+req.id + req.samples 
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
    override def fileTransfer(req: FileRequest) = {
      val reply = FileReply(
        result = ResultType.SUCCESS,
        message = "File transfer complete from master to " + req.ip,
        file = getLine()
      )

      MasterWorkerServer.logger.info(
        "[File Transfer] Input file reply to " + req.ip + ":" + req.port + " completed"
      )
      Future.successful(reply)
    }
  }

}
