package common

object WorkerState {
  // sealed abstract class WorkerState
  case object Init extends WorkerState
  case object Connected extends WorkerState
  case object Sampling extends WorkerState
  case object SortPartition extends WorkerState
  case object Shuffle extends WorkerState
  case object Merge extends WorkerState
  case object End extends WorkerState
}

// object ShuffleServerState{
//   case object ServerOn extends ShuffleServerState
//   case object ServerOff extends ShuffleServerState
// }

class WorkerState {}

class WorkerInfo(
    val ip: String,
    val port: Int
) {
  var workerState: WorkerState = WorkerState.Connected
  // var shuffleServerState: ShuffleServerState = ShuffleServerState.ServerOff

  def setWorkerState(state: WorkerState) = {
    workerState = state
  }
  // def setShuffleServerState(state: ShuffleServerState) = {
  //   shuffleServerState = state
  // }

}
