#!/bin/bash

# Remove old output files
echo "############################"
echo "STEP 1: REMOVE OLD OUTPUT"
echo "############################"
hadoop fs -rm -r out

# Execute hadoop jobs
echo "###################################"
echo "STEP 2: EXECUTE HADOOP TASK 3 TIMES"
echo "###################################"

counter=1
while [ $counter -le 3 ]
do
  hadoop fs -rm -r out
  start=`date +%s`
  python B.py -r hadoop --output-dir out hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/contracts

  # Echo how long the task took
  end=`date +%s`
  echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
  echo "Process took $((end-start)) seconds to complete."
  echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"

  # Collect output
  hadoop fs -getmerge out output_b.txt

  # Update the counter
  echo "Hadoop run $(($counter)) complete."
  ((counter++))
done

# Execute spark jobs
echo "###################################"
echo "STEP 3: EXECUTE SPARK TASK 3 TIMES"
echo "###################################"
counter=1
while [ $counter -le 3 ]
do
  start=`date +%s`

  # Run the spark task
  spark-submit B_Spark.py &> output_spark.txt

  # Echo how long the task took
  end=`date +%s`
  echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
  echo "Process took $((end-start)) seconds to complete."
  echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"

  # Update the counter
  echo "Spark run $(($counter)) complete."
  ((counter++))
done
