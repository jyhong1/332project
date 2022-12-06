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


object FileServer{
    private val logger = Logger.getLogger(classOf[FileServer].getName)
    private val port = 8000 /*TBD*/
    def apply(
      executionContext: ExecutionContext,
      numClients: Int
    ): FileServer = {
    new FileServer(executionContext, numClients)
  }
}

class FileServer(executionContext: ExecutionContext, numClients: Int) {
    self => 
        private[this] var server: Server = null
        private[this] var clientSet: Map[Int, (String, Int)] = Map()
    private val localhostIP = InetAddress.getLocalHost.getHostAddress

    def start(): Unit = {
        server = ServerBuilder
        .forPort(FileServer.port)
        .addService(ShuffleNetworkGrpc.bindService(new ShuffleNetworkImpl, executionContext))
        .build
        .start
        FileServer.logger.info("File Server started, listening on " + FileServer.port)
        sys.addShutdownHook {
        System.err.println("*** shutting down gRPC server since JVM is shutting down")
        self.stop()
        System.err.println("*** server shut down")
            }
    }

    def checkOnline(ip:String, port:Int):Boolean = {
        try{
            val s:Socket = new Socket(ip,port)
            true
        }catch{
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
        override def sendPartition(req: SendPartitionRequest) = {
            val addr = req.from match{
                case Some(from) => from
                case None       => sAddress(ip = "", port = 1)
            }
            FileServer.logger.info(
                "[Shuffle] Partition from" + addr.ip +":" + addr.port + " arrived"
                )
            var count: Int = 0
            Utils.createdir(req.outputpath)
            count.synchronized{
                
                for(i <- 0 to req.partitions.length-1){
                    val partition = req.partitions(i)
                    val writer = new BufferedWriter(new FileWriter(req.outputpath + "/" + req.filenames(i)))
                    for ( i <- 0 until partition.partition.length){
                        writer.write(partition.partition(i)+"\n")
                    }
                    writer.close()
                }
                FileServer.logger.info(
                    "[Shuffle] received partition from" + addr.ip
                )
                count+=1
            }

            if (Utils.waitWhile(() => count < numClients, 100000)){
                val reply = SendPartitionReply(
                    result = sResultType.SUCCESS
                )
                FileServer.logger.info(
                "[Shuffle] Complete to get Partition from " + addr.ip
                )
                Future.successful(reply)
            }else{
                val reply = SendPartitionReply(
                    result = sResultType.FAILURE
                )
                FileServer.logger.info(
                    "[Shuffle] shuffle server failed to get partition"
                )
                Future.successful(reply)
            }
            
        }
        
    }
}