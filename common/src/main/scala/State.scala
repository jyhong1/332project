package common

trait State {
  sealed abstract class workerState
  case object Init extends workerState
  case object Connected extends workerState
  case object Sampling extends workerState
  case object SortPartition extends workerState
  case object Shuffle extends workerState
  case object Merge extends workerState
  case object End extends workerState
}

class WorkerInfo(ip: String, port: Int) extends State {
  var workerState = Connected

}
