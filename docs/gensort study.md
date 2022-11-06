# gensort study

## inputdata 추출
[gensort data](http://www.ordinal.com/gensort.html)에서 input data 생성 가능
1. 압축 파일 저장 후 압축 해제
2. cd gensort-linux-1.5/64
3. /gensort option 의 형식으로 실행 가능

## work flow 정리
master필요기능
1. worker machine 순서 지정해주기(ip,port)값 다 다름
2. 전체 data input받아오기
3. worker 순서대로 data 갈라서 전달해주기

worker필요기능
4. input data key값 max,min 찾아서 master에 전달
5. input data 갈라서 disk 크기에 해당하는 partition으로 만들기

master 필요기능
6. 만들어진 partition들 전달받은 worker machine들의 key range에 맞게끔 전체 worker들 돌면서 찾아오는 식으로 merge해줌
7.data를 output machine에 순서대로 집어넣어줌

extra 필요기능
* 맞게 merge 된건지 output key range검사
* 각 단계(data 맞게들어왔는지, partition맞게만들어졌는지)마다 쿼리 맞게 시행된건지 alert

## Challenge
1. The input data does not fit in memory.
Solution: Implement disk-based merge sort
2. Allocate a fixed number of sort/partition threads because of disk I/O. However, the merge phase can use multiple threads.
Solution: Assign multiple partitions for each machines.
3. For distributed/parallel processing, we might need a number of machines. Therefore we need to use network communication to send/send back messages.
Solution: Use gRPC communication between machines.

