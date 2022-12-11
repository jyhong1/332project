# 332project - Red Team(jyhong1, daehuikim, JeongHunP)

## How to test?
1. git clone [our repo]
2. Go to our repo's directory(332project)
3. To run master, type-> sbt "run master {number of workers}"
4. To run worker, type-> 
sbt "run worker { masterIp:masterPort(Port initialized to 50051) } -I {path of input data} -O {output directory}
* Ex) If input directory exists in 332project/data1/input, {path of input data} will be /data1/input/ .
* Ex) master: sbt "run master 4"
      worker: sbt "run worker -I /data/input1/ /data/input2/ -O /data/output"
You can add various input files such as /data1/input/ /data2/input/ .
5. Result is sorted in {output directory} including several files.

## Test list
1. Test environment : vm servers (eg. 2.2.2.101:50051)
2. Test with 10 lines, 1MB, 32MB blocks. Total size of blocks will be increased while test.
3. Give input of one dir, severl dirs, and repeated dirs.
4. Verify with valsort.

## Milestones (*These might be changed)
--- 
<details>
<summary>General setup</summary>
<div markdown="1">
- Input data generation <br/>
- gRPC communication server,client setup <br/>
- Implement Master, Worker class <br/>
</div>
</details><br/>

<details>
<summary>Implement communication phase</summary>
<div markdown="1">
- Implement basic message system<br/>
- Implement server, client class<br/>
</div>
</details><br/>

<details>
<summary>Implement Sampling phase (Master)</summary>
<div markdown="1">
- Decide sample size<br/>
- Decide how to set key range<br/>
</div>
</details><br/>

<details>
<summary>Implement Sorting phase (Worker) (Distributed/Parallel phase)</summary>
<div markdown="1">
- Decide how to sort blocks<br/>
- Decide hot to prevent collapse (parallel programming)<br/>
</div>
</details><br/>

<details>
<summary>Implement Partitioning phase (Worker) (Distributed/Parallel phase)</summary>
<div markdown="1">
- Decide partition size <br/>
- Decide hot to prevent collapse (parallel programming)<br/>
</div>
</details><br/>

<details>
<summary>Implement Shuffle & Merge phase (Worker & Master)</summary>
<div markdown="1">
- Decide shuffle algorithm<br/>
- Decide sorting(merging) algorithm<br/>
</div>
</details><br/>

--- 

## Weekly progress report
1. [Week1](./Weekly%20Progress%20report/Week1.md)
2. [Week2](./Weekly%20Progress%20report/Week2.md)
3. [Week3](./Weekly%20Progress%20report/Week3.md)
4. [Week4](./Weekly%20Progress%20report/Week4.md)
5. [Week5](./Weekly%20Progress%20report/Week5.md)
6. [Week6](./Weekly%20Progress%20report/Week6.md)
7. [Week7](./Weekly%20Progress%20report/Week7.md)
8. [Week8](./Weekly%20Progress%20report/Week8.md)