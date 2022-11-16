#!/bin/bash

NUM_FILE=3
Index=10

echo "Generate dataset with $NUM_FILE partitions"

rm ./dataset
mkdir ./dataset

for i in $(seq 1 $NUM_FILE)
do
	#mkdir ./dataset/input
	let idx=($i-1)*$Index
	./Downloads/gensort-linux-1.5/64/gensort -a -b$idx $Index ./dataset/input$i
done

echo "Data generation complete"