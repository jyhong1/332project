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
  ResultType
}
import protos.network.NetworkGrpc.NetworkBlockingStub
import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}
import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel, Status}
import java.net.InetAddresssampler
import scala.concurrent.{Promise}
import com.google.protobuf.Duration
import phase.{sampleMaker}
import io.grpc.stub.StreamObserver

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
    try {
      val user = args.headOption.getOrElse("Team Red!")
      val inputPath = "."
      val sampleSize = 10
      val samples:List[String] = List()
      
      /*connection request*/
      client.connect(user)

      /*make samples*/
      samples::client.makeSamples(inputPath,sampleSize)

      /*send samples*/
      val samplePromise = Promise[Unit]()
      client.sendSamples(samplePromise)
      Await.ready(samplePromise.future, Duration.Inf)

      /*following phase will be added*/

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
    logger.info("[Shutdonw] Client Shutdown")
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  /** Say hello to server. */
  def connect(name: String): Unit = {
    val localhostIP = InetAddress.getLocalHost.getHostAddress
    val port = 8000
    logger.info("[Connection]: Start to connect to master from " + name)

    val request = ConnectionRequest(ip = localhostIP, port = port)
    try {
      val response = blockingStub.connection(request)
      logger.info("[Connection]: " + response.message)
    } catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
    }
  }

  /*Sample phase: make samples*/
  def makeSamples(inputPath: String, sampleSize: Int): Unit ={
    logger.info("[Sample] First half start")
    val samples = sampleMaker.sampling(inputPath,sampleSize)
    logger.info("[Sample] First half done")
    samples
  }

  /*Sample phase: send samples*/
  final def sendSamples(samplePromise: Promise[Unit]): Unit= {
    logger.info("[Sample] Try to send samples to Master")

    val responseObserver = new StreamObserver[SamplingReply](){
      override def onNext(response: SamplingReply): Unit = {
        if (response.result == ResultType.SUCCESS){
          samplePromise.success()
        }
      }
      override def onError(t: Throwable): Unit = {
        logger.warning(s"[Sample] Server response Failed: ${Status.fromThrowable(t)}")
        samplePromise.failure(new WorkerFailedException)
      }

      override def onCompleted(): Unit = {
        logger.info("[Sample] Done sending sample")
      }
  }
  val replyingObserver =blockingStub.sampling(responseObserver)
  try{
    val reply = SamplingRequest(id=id,samples=samples)
    replyingObserver.onNext()
  } catch{
    case e: RuntimeException => {
        // Cancel RPC
        replyingObserver.onError(e)
        throw e
      }
  }
  replyingObserver.onCompleted()
}
}