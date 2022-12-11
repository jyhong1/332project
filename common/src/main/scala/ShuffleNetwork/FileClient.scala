package shufflenetwork

import shuffle.shuffle.{
    sAddress,
    sResultType,
    ShuffleNetworkGrpc,
    SendPartitionRequest,
    SendPartitionReply
}
import io.grpc.{ManagedChannel, StatusRuntimeException, ManagedChannelBuilder}
import java.net.InetAddress
import shuffle.shuffle.ShuffleNetworkGrpc.ShuffleNetworkBlockingStub
import java.util.logging.{Level, Logger}
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.collection.mutable.Buffer
import java.io.File
import common.Utils
import scala.concurrent.{Promise,Await}
import scala.concurrent.duration._
import io.grpc.stub.StreamObserver
import com.google.protobuf.ByteString

object FileClient {
  def apply(host: String, port: Int, inputFilePaths: List[String], to: String): FileClient = {
    val channel =
      ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = ShuffleNetworkGrpc.blockingStub(channel)
    new FileClient(channel, blockingStub,inputFilePaths,to)
  }
}

class FileClient(
    private val channel: ManagedChannel,
    private val blockingStub: ShuffleNetworkBlockingStub,
    private val inputFilePaths: List[String],
    private val to: String
) {
  val localhostIP = InetAddress.getLocalHost.getHostAddress
  val port = 8000
  val asyncStub = ShuffleNetworkGrpc.stub(channel)
  val choosenFiles = inputFilePaths.filter(_.split("/").last.split("_")(1) == to)

  private[this] val logger =
    Logger.getLogger(classOf[FileClient].getName)

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  def shuffling():Unit = {
    for (file <- choosenFiles){
      val shufflePromise = Promise[Unit]()
      sendPartition(file, to, shufflePromise)
      Await.ready(shufflePromise.future,Duration.Inf)
    }
    shutdown()
  }

  def sendPartition(file: String, to: String, shufflePromise: Promise[Unit]): Unit = {
    logger.info("[Shuffle] Try to send partition from" + localhostIP + "to" + to)
    val replyObserver = new StreamObserver[SendPartitionReply]() {
      override def onNext(reply: SendPartitionReply): Unit = {
        if (reply.result == sResultType.SUCCESS) {
          shufflePromise.success()
        }
      }
      override def onError(t: Throwable): Unit = {
        logger.warning("[Shuffle] sending failed")
        shufflePromise.failure(new Exception)
      }
      override def onCompleted(): Unit = {
        logger.info("[Shuffle] Done sending partition")
      }
    }
    val requestObserver = asyncStub.sendPartition(replyObserver)
    try {
      val source = Source.fromFile(file)
      val filename = file.split("/").last
      for (line <- source.getLines) {
        val request = SendPartitionRequest(Some(sAddress(localhostIP, port)),ByteString.copyFromUtf8(line+"\r\n"),filename)
        requestObserver.onNext(request)
      }
      source.close
    } catch {
      case e: RuntimeException => {
        // Cancel RPC
        requestObserver.onError(e)
        throw e
      }
    }
    // Mark the end of requests
    requestObserver.onCompleted()
  }
}
