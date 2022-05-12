#!/bin/bash

# execute spark job
echo "############################"
echo "    EXECUTING SPARK TASK    "
echo "############################"
spark-submit C_Spark.py &> output_c.txt
