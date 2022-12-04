# Week7
## 1. Progress in the previous week
### jyhong1(홍재영)
* Merge code with sending file & (Master worker connection) [O]
Implemented MergeFile function in worker.scala that merges partition files.
* Implement file related libraries if necessary [X]
Maybe libraries are not necessary, make function separately.
* Study about sorting method [O]
Sorting function implemented.

### daehuikim(김대희)
* Complete implementation of sampling phase [O] 
[sampling phase 1](https://github.com/jyhong1/332project/pull/21)
[sampling phase 2](https://github.com/jyhong1/332project/pull/25)
* PR and code review[O]
PRs are on the above (sampling), code review(merge)
you can check in our PRs
* Implement remaining phases [*]
Working on shuffling phase
    * shuffle ready [O]
    * send partition [*] => test and debug
    * shuffle complete [O]

### JeongHunP(박정훈)
*  Complete protobuf classes for each phases [O]
*  Implement overall grpc methods and test connection/file transfer [O]
*  Implement some phases(Connection, Sort) [O]
*  Change all of the structures of codes and connect other member's code [O]
*  Synchronize worker's reply in server [O]
*  Set up reading and writing directories [O]

## 2. Goal of the week
1. Complete implementation of remaining phases (shuffle, partition)
2. Test with generated data
3. Refactor codes, publish docs

## 3. Goal of the week for each individual member
### jyhong1(홍재영)
* Implement partitioning with refactored codes.
* After implementing partitioning, connect with shuffling.
* Test with big inputs.

### daehuikim(김대희)
* Complete remaining impletation
* Merge whole project and test

### JeongHunP(박정훈)
*  Implement block for storing data
*  Implement thread-based worker and assign multiple key ranges to workers
*  Refractor and merge whole flow of codes