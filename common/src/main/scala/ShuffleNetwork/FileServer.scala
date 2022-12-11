package shufflenetwork

import shuffle.shuffle.{
  sAddress,
  sResultType,
  ShuffleNetworkGrpc,
  SendPartitionRequest,
  SendPartitionReply
}

import java.util.logging.Logger
import scala.concurrent.{ExecutionContext, Future}
import io.grpc.{Server, ServerBuilder, Status}
import java.net.InetAddress
import java.net.Socket
import java.io.IOException
import java.io.File
import java.nio.file.Paths
import java.nio.file.Files
import java.io.BufferedWriter
import java.io.FileWriter
import common.Utils
import java.io.FileOutputStream
import io.grpc.stub.StreamObserver

object FileServer {
  private val logger = Logger.getLogger(classOf[FileServer].getName)
  private val port = 8000 /*TBD*/
  def apply(
      executionContext: ExecutionContext,
      numClients: Int,
      outputPath: String
  ): FileServer = {
    new FileServer(executionContext, numClients, outputPath)
  }
}

class FileServer(executionContext: ExecutionContext, numClients: Int, outputPath: String) {
  self =>
  private[this] var server: Server = null
  private[this] var clientSet: Map[Int, (String, Int)] = Map()
  private val localhostIP = InetAddress.getLocalHost.getHostAddress
  Utils.createdir(outputPath)

  def start(): Unit = {
    server = ServerBuilder
      .forPort(FileServer.port)
      .addService(
        ShuffleNetworkGrpc.bindService(new ShuffleNetworkImpl, executionContext)
      )
      .build
      .start
    FileServer.logger.info(
      "File Server started, listening on " + FileServer.port
    )

    sys.addShutdownHook {
      System.err.println(
        "*** shutting down gRPC server since JVM is shutting down"
      )
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  def checkOnline(ip: String, port: Int): Boolean = {
    try {
      val s: Socket = new Socket(ip, port)

      true
    } catch {
      case e: IOException =>
        FileServer.logger.warning("server is not online!")

        false
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

  private class ShuffleNetworkImpl extends ShuffleNetworkGrpc.ShuffleNetwork {
    override def sendPartition(replyObserver: StreamObserver[SendPartitionReply]): StreamObserver[SendPartitionRequest] = {
      new StreamObserver[SendPartitionRequest] {
        var workerip:String = ""
        var workerport:Int = -1
        var writer: FileOutputStream = null

        override def onNext(request: SendPartitionRequest): Unit = {
          workerip = request.from.get.ip
          workerport = request.from.get.port
          val recievedfile = request.filename
          if (writer == null) {
            FileServer.logger.info(s"[ShuffleServer]: getting from $workerip with partition $recievedfile")
            val filename = outputPath + "/" + recievedfile
            val file = new File(filename)
            file.createNewFile()
            writer = new FileOutputStream(file)
          }
          request.item.writeTo(writer)
          writer.flush
        }

        override def onError(t: Throwable): Unit = {
          FileServer.logger.warning("[ShuffleServer]: Worker failed to send partition")
          throw t
        }

        override def onCompleted(): Unit = {
          FileServer.logger.info("[ShuffleServer]: Worker done sending partition")
          writer.close
          replyObserver.onNext(new SendPartitionReply(sResultType.SUCCESS))
          replyObserver.onCompleted
        }
      }
    }
  }
}
