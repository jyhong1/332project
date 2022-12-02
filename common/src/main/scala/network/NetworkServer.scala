package network

import protos.network.{
  NetworkGrpc,
  Address,
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
import io.grpc.stub.StreamObserver;
import java.net.InetAddress
import java.net._
import java.io.{OutputStream, FileOutputStream, File}
import scala.sys.process._
import scala.io.Source
import com.google.protobuf.ByteString
import rangegenerator.keyRangeGenerator

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
  private[this] var clientSet: Map[Int, (String, Int)] = Map()

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

  /// master's functions

  // read input file and convert to String, Return input data into type String
  def getLine(): String = {
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
      val addr = req.addr match {
        case Some(addr) => addr
        case None       => Address(ip = "", port = 1) // TODO: error handling
      }
      NetworkServer.logger.info(
        "[Connection] Request from " + addr.ip + ":" + addr.port + " arrived"
      )

      // TODO: save worker information

      // can give pending?
      val reply = ConnectionReply(
        result = ResultType.SUCCESS,
        message = "Connection complete to master from " + addr.ip,
        file = getLine()
      )

      NetworkServer.logger.info(
        "[Connection] Input file reply to " + addr.ip + ":" + addr.port + " completed"
      )
      Future.successful(reply)
    }

    override def merge(req: MergeRequest) = {
      val reply = MergeReply(
        message = "Connection complete from "
      )
      Future.successful(reply)
    }
    override def sampling(req: SamplingRequest) = {
      val addr = req.addr match {
        case Some(addr) => addr
        case None       => Address(ip = "", port = 1) // TODO: error handling
      }
       NetworkServer.logger.info(
        "[Sampling] Sampling Request from " + addr.ip + ":" + addr.port + " arrived"
      )
       NetworkServer.logger.info(
        "[Sampling] test log about sampling " + req.samples.head + " arrived"
       )

      // TODO: sync and make keys
      
      val keyranges:Seq[(String, String)] = new keyRangeGenerator(req.samples,2).generateKeyrange() /*num workers required*/

      val reply = SamplingReply(
        result = ResultType.SUCCESS,
      )

      NetworkServer.logger.info(
        "[Sampling] sampling completed from " + addr.ip + ":" + addr.port + req.samples
      )
      Future.successful(reply)
    }

    override def shuffle(req: ShuffleRequest) = {
      val reply = ShuffleReply(
        message = "Connection complete from "
      )
      Future.successful(reply)
    }
    override def sortPartition(req: SortPartitionRequest) = {
      val addr = req.addr match {
        case Some(addr) => addr
        case None       => Address(ip = "", port = 1) // TODO: error handling
      }

      // TODO: sync and organize the sequence of file transfer
      NetworkServer.logger.info(
        "[Sort/Partition] sort/partition completed from " + addr.ip + ":" + addr.port
      )

      val reply = SortPartitionReply(
        message = "Please start to transfer partitions"
        // TODO: give sequence
      )
      Future.successful(reply)
    }

    override def fileTransfer(request: FileRequest): Future[FileReply] = ???
  }
}
