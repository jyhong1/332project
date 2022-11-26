package gensort

import gensort.master.MasterWorkerServer
import gensort.worker.MasterWorkerClient

object Main {
  def main(args: Array[String]): Unit = {
    require(args.length >= 2 && (args(0) == "master" || args(0) == "worker"))
    val machineType = args(0)
    val newArgs = args.slice(1, args.length)
    if (machineType == "master") {
      MasterWorkerServer.main(newArgs)
    } else if (machineType == "worker") {
      MasterWorkerClient.main(newArgs)
    } else {
      println("Illegal Arguments")
    }
  }
}
