package gensort.worker

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
  FileReply,
  FileRequest,
  ResultType
}
import protos.network.NetworkGrpc.NetworkBlockingStub
import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}
import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}
import java.net.InetAddress
import java.io.{OutputStream, FileOutputStream, File, IOException}
import java.nio.file.{Files, Path, Paths}

object MasterWorkerClient {
  def apply(host: String, port: Int): MasterWorkerClient = {
    val channel =
      ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = NetworkGrpc.blockingStub(channel)
    new MasterWorkerClient(channel, blockingStub)
  }

  def main(args: Array[String]): Unit = {
    /*require(
      args.length >= 5 && args(0).contains(":") && args(1) == "-I" && args(
        args.length - 2
      ) == "-O"
    )*/
    val master = args(0).split(":")
    val client = MasterWorkerClient(master(0), master(1).toInt)

    val inputPath = System.getProperty("user.dir") + "data/input"
    try {
      val user = args.headOption.getOrElse("Team Red!")
      client.connect(user)
      client.requestFile(inputPath)
    } finally {
      client.shutdown()
    }
  }
}

class MasterWorkerClient private (
    private val channel: ManagedChannel,
    private val blockingStub: NetworkBlockingStub
) {
  private[this] val logger =
    Logger.getLogger(classOf[MasterWorkerClient].getName)

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  /** Say hello to server. */
  def connect(name: String): Unit = {
    val localhostIP = InetAddress.getLocalHost.getHostAddress
    val port = 8000
    logger.info("[Connection]: Start to connect to master " + name)

    val request = ConnectionRequest(ip = localhostIP, port = port)
    try {
      val response = blockingStub.connection(request)
      logger.info("[Connection]: " + response.message)
    } catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
    }
  }

  // (Request input data to Server), (After get input, make dir and save as a file)
  def requestFile(inputdir: String): Unit = {
    logger.info("Request input file")
    val request = FileRequest(inputdir = inputdir)

    try {
      val response = blockingStub.fileTransfer(request)
      logger.info("Received input\n" + response.file)

      val dir = new File("./data/received")
      if (!dir.exists()) {
        dir.mkdir()
      }

      val file = new File("./data/received/receivedInput")
      file.createNewFile()

      val path = Paths.get("./data/received/receivedInput")
      Files.write(path, response.file.getBytes())
    } catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
    }
  }
}
