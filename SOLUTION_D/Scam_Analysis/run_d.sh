#!/bin/bash

# Remove old output files
echo "#################################"
echo "STEP 1: REMOVE OLD OUTPUT"
echo "#################################"
rm output_d.txt
hadoop fs -rm -r output

# Execute json conversion task
echo "###################################"
echo "STEP 2: CONVERT JSON TO CSV        "
echo "###################################"
python Scam_Converter.py

# Execute json conversion task
echo "###################################"
echo "STEP 3: RUN SCAM SPARK             "
echo "###################################"
spark-submit Scam_Analysis.py

# copy output to local directory
echo "#################################"
echo "STEP 4: COLLECT OUTPUT FILES"
echo "#################################"
hadoop fs -getmerge output output_d.txt

# copy output to local directory
echo "#################################"
echo "STEP 5: GENERATE PLOTS AND TABLES"
echo "#################################"
python Scam_Plot.py
