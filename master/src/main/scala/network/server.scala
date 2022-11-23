package network

import scala.concurrent.{ExecutionContext, Future, Promise, Await}

class server(executionContest : ExecutionContextm, port: Int, requiredWorkerNum: Int) {
    self => require(requiredWorkerNum > 0, "requiredWorkerNum should be positive")
    
    def start(): Unit = {
        
    }
}
