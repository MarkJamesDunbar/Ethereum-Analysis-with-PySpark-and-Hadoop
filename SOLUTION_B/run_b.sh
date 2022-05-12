#!/bin/bash

# Remove old output files
echo "############################"
echo "STEP 1: REMOVE OLD OUTPUT"
echo "############################"
hadoop fs -rm -r out

# execute hadoop job
echo "############################"
echo "STEP 2: EXECUTE HADOOP TASK"
echo "############################"
python B.py -r hadoop --output-dir out hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/contracts

#copy output to local directory
echo "############################"
echo "STEP 3: COLLECT OUTPUT FILES"
echo "############################"
hadoop fs -getmerge out output_b.txt
