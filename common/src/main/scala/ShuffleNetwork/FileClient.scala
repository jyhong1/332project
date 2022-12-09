package shufflenetwork

import shuffle.shuffle.{
    sAddress,
    sResultType,
    partitionFile,
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

object FileClient {
  def apply(host: String, port: Int): FileClient = {
    val channel =
      ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = ShuffleNetworkGrpc.blockingStub(channel)

    new FileClient(channel, blockingStub)
  }
}

class FileClient(
    private val channel: ManagedChannel,
    private val blockingStub: ShuffleNetworkBlockingStub
) {
  val localhostIP = InetAddress.getLocalHost.getHostAddress
  val port = 8000

  private[this] val logger =
    Logger.getLogger(classOf[FileClient].getName)

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }


  def sendPartition(to: String, inputpaths: List[String], outputpath: String): SendPartitionReply = {
    logger.info("[Shuffle] Try to send partition from" + localhostIP + "to" + to)
    val fromaddr = sAddress(localhostIP, port)
    val choosenFiles = inputpaths.filter(_.split("/").last.split("_")(1) == to)
    val filenames = choosenFiles.map(file => file.split("/").last)
    var partitions:Seq[partitionFile] = Seq() 
    for(i <- choosenFiles){
      partitions = partitions :+ Utils.getPartitionFile(i)
    }
    val request = SendPartitionRequest(Some(fromaddr), partitions, filenames.toSeq, outputpath)
    try{
        val response = blockingStub.sendPartition(request)
        response
    }catch{
        case e: StatusRuntimeException =>

        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)

        SendPartitionReply(sResultType.FAILURE)
    }
  }
}
