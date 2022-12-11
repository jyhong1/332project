package network

import protos.network.{
  NetworkGrpc,
  Address,
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
  RangeRequest,
  RangeReply,
  ResultType
}

import protos.network.NetworkGrpc.{NetworkBlockingStub, NetworkStub}
import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}
import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}
import io.grpc.stub.StreamObserver
import io.grpc.{Status}

import java.net.InetAddress
import java.io.{OutputStream, FileOutputStream, File, IOException}
import java.nio.file.{Files, Path, Paths}
import scala.concurrent.{Promise, Await}
import scala.concurrent.duration._
import scala.io.Source
import com.google.protobuf.ByteString
import scala.concurrent._

object NetworkClient {
  def apply(host: String, port: Int): NetworkClient = {
    val channel =
      ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = NetworkGrpc.blockingStub(channel)
    new NetworkClient(channel, blockingStub)
  }
}

class NetworkClient private (
    private val channel: ManagedChannel,
    private val blockingStub: NetworkBlockingStub
) {
  val id: Int = -1
  val localhostIP = InetAddress.getLocalHost.getHostAddress
  val port = 9000
  val asyncStub = NetworkGrpc.stub(channel)

  private[this] val logger =
    Logger.getLogger(classOf[NetworkClient].getName)

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  def connect(address: String): ConnectionReply = {
    logger.info(
      "[Connection]: Start to connect to Master server " + address
    )

    val addr = Address(localhostIP, port)
    val request = ConnectionRequest(Some(addr))
    try {
      val response = blockingStub.connection(request)
      logger.info(
        "[Connection]: " + response.message
      )

      response
    } catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)

        ConnectionReply(ResultType.FAILURE)
    }
  }

  def sendSamples(
      samplePromise: Promise[Unit],
      inputFile: String
  ): Future[Unit] = {
    val p = Promise[Unit]()
    logger.info("[sendSamples] Try to send sample")
    val replyObserver = new StreamObserver[SamplingReply]() {
      override def onNext(reply: SamplingReply): Unit = {
        if (reply.result == ResultType.SUCCESS) {
          samplePromise.success()
        }
      }
      override def onError(t: Throwable): Unit = {
        logger.warning(
          s"[requestSample] Server response Failed: ${Status.fromThrowable(t)}"
        )
        samplePromise.failure(new Exception)
      }
      override def onCompleted(): Unit = {
        logger.info("[sendSamples] Done sending sample")
        p.success(())
      }
    }
    val requestObserver = asyncStub.sampling(replyObserver)
    try {
      val source = Source.fromFile(inputFile)
      for (line <- source.getLines) {
        val request = SamplingRequest(
          addr = Some(Address(localhostIP, port)),
          sample = ByteString.copyFromUtf8(line)
        )
        requestObserver.onNext(request)
      }
      source.close
    } catch {
      case e: RuntimeException => {
        requestObserver.onError(e)
        throw e
      }
    }
    requestObserver.onCompleted()
    p.future
    
  }

  def getRange(): RangeReply = {
    logger.info("[Range] Try to broadcast Ranges from Master")
    val addr = Address(localhostIP, port)
    val request = RangeRequest(Some(addr))
    try {
      val response = blockingStub.range(request)
      response
    } catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
        RangeReply(ResultType.FAILURE)
    }
  }

  def sortPartitionComplete(): SortPartitionReply = {
    logger.info(
      "[Sort/Partition] Try to send finish message to Master server"
    )

    val addr = Address(localhostIP, port)
    val request = SortPartitionRequest(Some(addr))
    try {
      val response = blockingStub.sortPartition(request)
      logger.info(
        "[Sort/Partition]" + response.message
      )

      response
    } catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
        SortPartitionReply(ResultType.FAILURE)
    }
  }

  def checkShuffleReady(state: Boolean): ShuffleReadyReply = {
    logger.info(
      "[Shuffle] Try to send Shuffle ready to Master"
    )

    val addr = Address(localhostIP, port)
    val request = ShuffleReadyRequest(Some(addr))

    try {
      val response = blockingStub.shuffleReady(request)
      logger.info(
        "[shuffle] Connect Status: " + response.result
      )

      response
    } catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)

        ShuffleReadyReply(ResultType.FAILURE)
    }
  }

  def checkShuffleComplete(state: Boolean): ShuffleCompleteReply = {
    logger.info(
      "[Shuffle] Try to send Master shuffle complete"
    )
    val addr = Address(localhostIP, port)
    val request = ShuffleCompleteRequest(Some(addr), state)
    try {
      val response = blockingStub.shuffleComplete(request)
      logger.info(
        "[Shuffle] complete arrange every partitions at" + addr.ip
      )
      response
    } catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)

        ShuffleCompleteReply(ResultType.FAILURE)
    }
  }

  def mergeComplete(): MergeReply = {
    logger.info(
      "[Merge] Try to send finish message to Master server"
    )
    val addr = Address(localhostIP, port)
    val request = MergeRequest(Some(addr))
    try {
      val response = blockingStub.merge(request)
      logger.info(
        "[Merge]" + response.message
      )

      response
    } catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
        MergeReply(ResultType.FAILURE)

    }
  }
}
