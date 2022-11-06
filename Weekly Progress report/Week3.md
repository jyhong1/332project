# Week3
## 1. Progress in the previous week

we set milestones for our projects like below. (*These might be changed)
1. General setup (input data, master<=>worker communication setup)
2. Find or Implement sorting libraries
3. Implement Sampling, Partition, Shuffle stage
4. gRPC communication error handling
5. test and analyze output data and time

### jyhong1(홍재영)
* Advance understanding about overall structure of the proejct [O]
- Breif understanding about Sampling, Pivoting, Sorting & Partition, Merge
* Finished setup - Ubuntu, intellij
* Make a test of gensort about making example data(key)


### daehuikim(김대희)
* gensort algorithm study [O] - [gensort study note](/docs/gensort%20study.md)
* scala project build study [O]
1. build a empty project using intelliJ environment
2. install [sbt](https://www.scala-sbt.org/download.html)
3. use commands starting with sbt (ex- sbt build, sbt run, sbt test)
4. more functions [about sbt](https://www.scala-sbt.org/1.x/docs/)

### JeongHunP(박정훈)
* gensort algorithm study [O] - understand the workflow of the project
* research additional network structure [O] - grpc/protobuf base study [ScalaPB](https://scalapb.github.io/)
* implement base structure of network and setup [X] - will be done until next week

## 2. Goal of the week
* Generate input data with small size
* Search sorting libraries & implement own Sorting library.
* Reach to the first milestone we set.

## 3. Goal of the week for each individual member

### jyhong1(홍재영)
* Analyze about sorting method including merge.
* Make Master and Worker which has individual ip address.

### daehuikim(김대희)
* Draw gensort architecture phases parellely.
* study and implement general setup(MS#1)

### JeongHunP(박정훈)
* study grpc/protobuf base structure of our project
* implement basic request/response of grpc between master and workers
* the documents will be organized and uploaded to project github files
