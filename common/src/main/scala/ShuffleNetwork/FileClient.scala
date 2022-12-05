package shufflenetwork

import shuffle.shuffle.{
    sAddress,
    sResultType,
    ShuffleNetworkGrpc,
    SendPartitionRequest,
    SendPartitionReply
}
import io.grpc.{ManagedChannel,StatusRuntimeException,ManagedChannelBuilder}
import java.net.InetAddress
import shuffle.shuffle.ShuffleNetworkGrpc.ShuffleNetworkBlockingStub
import java.util.logging.{Level, Logger}
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.collection.mutable.Buffer
import java.io.File

object FileClient{
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
  val id: Int = -1
  val localhostIP = InetAddress.getLocalHost.getHostAddress
  val port = 9000

  private[this] val logger =
    Logger.getLogger(classOf[FileClient].getName)

    def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  def getFile(filename: String): Seq[String] = {
    var partition:Buffer[String]=Buffer()
    for (line <- Source.fromFile(filename).getLines())
    {
        partition = partition:+line
    }
    partition
  }

  def sendPartitions(to: String, filename: String): SendPartitionReply = {
    logger.info("[Shuffle] Try to send partition from" + localhostIP + "to" + to)
    val fromaddr = sAddress(localhostIP, port)
    val toaddr = sAddress(to, 9000)
    val partition = getFile(filename)
    val path = filename
    val request = SendPartitionRequest(Some(fromaddr), Some(toaddr), partition, path)
    new File(filename).delete()

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