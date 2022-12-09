package gensort.master

import network.NetworkServer
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

object Master {
  def main(args: Array[String]): Unit = {
    // should be args len 1 which is master 3(num of workers)
    require(args.length == 1 && args(0).toInt >= 1 && args(0).toInt <= 10)
    val numWorkers = args(0).toInt

    val server = NetworkServer(ExecutionContext.global, numWorkers)

    try {
      server.start()
      server.blockUntilShutdown()
    } catch {
      case ex: Exception => println(ex)
    } finally{
      server.stop()
    }
  }
}
