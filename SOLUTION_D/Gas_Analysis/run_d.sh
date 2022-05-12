#!/bin/bash

# Remove old output files
echo "#################################"
echo "STEP 1: REMOVE OLD OUTPUT"
echo "#################################"
rm output_d_1.txt
rm output_d_2.txt
rm output_d_3.txt
hadoop fs -rm -r output
hadoop fs -rm -r output_2
hadoop fs -rm -r output_3

echo "###################################"
echo "STEP 2: RUN GAS PRICE SPARK        "
echo "###################################"
spark-submit Gas_Spark.py

echo "###################################"
echo "STEP 3: RUN GAS CORRELATION SPARK  "
echo "###################################"
spark-submit Gas_Correlation.py

echo "###################################"
echo "STEP 4: RUN GAS COMPLEXITY SPARK  "
echo "###################################"
spark-submit Gas_Complexity.py

# copy output to local directory
echo "#################################"
echo "STEP 5: COLLECT OUTPUT FILES"
echo "#################################"
hadoop fs -getmerge output output_d_1.txt
hadoop fs -getmerge output_2 output_d_2.txt
hadoop fs -getmerge output_3 output_d_3.txt

# plot the data
echo "#################################"
echo "STEP 5: COLLECT OUTPUT FILES"
echo "#################################"
python Gas_Plots.py
