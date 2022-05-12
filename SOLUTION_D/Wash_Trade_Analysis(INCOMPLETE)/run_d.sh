#!/bin/bash

# Remove old output files
echo "#################################"
echo "STEP 1: REMOVE OLD OUTPUT"
echo "#################################"
rm output_d.txt
hadoop fs -rm -r output

echo "###################################"
echo "STEP 2: RUN WASH TRADE SPARK       "
echo "###################################"
spark-submit Wash_Trade_Analysis.py #&> output_d.txt

# copy output to local directory
echo "#################################"
echo "STEP 3: COLLECT OUTPUT FILES"
echo "#################################"
# hadoop fs -getmerge output output_d.txt
