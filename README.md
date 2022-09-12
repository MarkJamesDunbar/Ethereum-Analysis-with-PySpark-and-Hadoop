# Ethereum-Analysis-with-PySpark-and-Hadoop
Project seeking to analyse all ethereum data from the first few transactions to the present. Below is a brief discussion of the results produced, the full report can be found inside this repository.

## Introduction

This report details use of Apache Spark and Hadoop tasks, to effectively analyse and process large volumes of data. This coursework is based on data from the Ethereum cryptocurrency network, between the years 2015 and 2019, and contains information on events in the network such as transactions, contracts, and active miners. The analysis tasks are split into four main tasks, detailed below in parts A, B, C and D. Please note that the Job IDs of each task are listed at the end of this report. Please also note that the code provided with this report submission is well documented and should be considered alongside the examples provided in this report. 

## Bash Scripting
For each of the sections detailed below, a bash script is used to run the Hadoop/Spark tasks and obtain any output as a “.txt” file. If necessary, the bash scripts will be used to run any further data processing and/or clean-up required; for example, the PyPlot-created data visualisations required in parts A and D. 

## A Part 1 – Number of Ethereum transactions per month
The first part of this project consists of two sub-tasks aimed at visualising the monthly transactional traffic occurring in the Ethereum network. For each of the tasks below, a python script containing a Hadoop MRJob Map-Reduce task is used. 
The resulting bar plot, generated using the Pandas and PyPlot modules in python is displayed below, along with the code used to produce this plot. 

![image](https://user-images.githubusercontent.com/57494763/168137243-1738eaea-d729-46ee-bdc4-a444601a6c6b.png)

In order to process the output data, a Pandas data-frame is used to read the “output.txt” file, using the tab (“\t”) character as a delimiter. This code can be seen in the files submitted with this coursework. The columns of the Pandas data-frame are then renamed to reflect the data present in each column for clarity, and any unnecessary characters are removed (for example, quotation marks). Finally, the date column in the data-frame is converted to a datetime, and the dates are sorted using the “sort_values” method in pandas, so that the data is ordered by month and year. 
The data is then plotted, producing the graph shown on the right above. This graph shows us that Ethereum transactions began to rise in early 2017, peaking around January 2018 at a value of approx. 3.5e^7 transactions in that month. The number of transactions reduced beyond January 2018, reaching a local minimum of around 1.5e^7transactions in February 2019. The number of transactions from there has risen steadily until the end of our dataset, reaching approximately 2.5e^7 transactions in June 2019.

## A Part 2 – Average value of Ethereum transactions per month
The resulting plot, generated using the Pandas and PyPlot modules in python, is displayed below, along with the code used to produce the plot shown below. 

![image](https://user-images.githubusercontent.com/57494763/168137432-1c41d028-85b5-406f-8b94-9289d6cfe68b.png)

The production of the plot for A part 2 is very similar to the method used in A part 1. To produce the plot from the output data, a Pandas data-frame is again used to read the output “.txt” file, using the tab (“\t”) character as a delimiter. The columns of the Pandas data-frame are then renamed to reflect the data present in each column for clarity, and any unnecessary characters are removed (for example, quotation marks). Finally, the date column in the data-frame is converted to a datetime, and the dates are sorted using the “sort_values” method in pandas, so that the data is ordered by month and year. 
The produced graph shows that the average value of transactions in the network decreases dramatically from June 2015 – at an initial value of 500ETH per transaction. For the months following, the average value of transactions hovers below an average of 100ETH per transaction each month.  A further peak is then seen in March 2017, where an average transaction value of approximately 200ETH is recorded. From January 2018 onwards, the Average transaction value hovers around 0ETH, indicating that there are no large transactions occurring within the network.

## B Part 1 - Top ten most popular services
To complete part B of the Ethereum analysis project, the Ethereum dataset was processed using MapReduce, producing a list of the top 10 services used. To do this, both the transactional and contractual datasets were used.
The produced list of the top ten services used in the network is shown below. The top-ranking service’s processed Ethereum value is almost double the second highest service, and approx. 10 times the size of the lowest ranking service in this list.

![image](https://user-images.githubusercontent.com/57494763/168137608-4c6fc3cf-98be-4f10-8eee-4046e387dc6d.png)

## C Part 1 - Top ten most active miners
To complete part C of the Ethereum analysis project, the Ethereum dataset was processed using Spark, producing a list of the top 10 most active Ethereum miners on the network. This analysis requires a Spark task that processes data from Ethereum block data held in the Hadoop filesystem.
The produced list of top ten most active miners is shown below.

![image](https://user-images.githubusercontent.com/57494763/168137885-5597d7b9-f83b-441f-821a-2e302d2bdc22.png)

## D Part 1 - Spark and Hadoop Comparative Analysis
To explore the differences in performance between Spark and Hadoop in performing the same task, a Spark task that obtained the same results as the task in part B of this project was generated. This python script is detailed below. 
This Spark process was run 3 times to obtain an average processing time. This was also done for the Hadoop task. The results of each time are recorded below, along with the average time taken across the three runs for each program. The time taken for each task to complete was calculated by “echoing” the current time in the console and calculating the time difference between the time at the start of each task, and the time at the end. 

![image](https://user-images.githubusercontent.com/57494763/168138057-ec1435ed-3c1a-43de-865e-c8b02940c312.png)

It should be noted that these runs were performed over an SSH connection, so the time taken to perform each task may not give the most accurate reflection of the performance of both the Hadoop and Spark task times. Note also that the results from the Spark file were printed, with an example output in the terminal shown below.

## D Part 2 - Most Popular Scams
Using a dataset of known scams – which includes information on Ethereum scam schemes, as well as the transactional dataset for Ethereum, an analysis of the most popular types of scams can be conducted. The aim of this analysis was to identify whether there is any correlation between the most lucrative scams and their statuses.
From the table generated for the scam popularity analysis below, Phishing is the most popular category, followed by Scamming, and finally Fake ICO when the status of each scam type is considered.  Roughly half of the total scams under the “Scamming” category are offline. 

![image](https://user-images.githubusercontent.com/57494763/168138217-acc5eae2-7698-4e4e-8b18-e1740daa486d.png)

The total volume of Ethereum each scam category has accumulated regardless of status can be summed. The Scamming category here is the most popular, followed by Phishing, and finally Fake ICO. Importantly, it can be seen that although Fake ICOs have yielded an overall lower volume – the average volume per ICO Scam is much higher, possibly indicating that while there are not many Fake ICO schemes, they yield large volumes in each transaction.

![image](https://user-images.githubusercontent.com/57494763/168138294-e255957d-36d7-45a3-ba6e-67542ff500e8.png)

## D Part 3 - Gas Price Analysis
In this section, three tasks are performed to analyse changes in gas price for Ethereum over the years, as well as the change in the complexity (gas used) of contracts. The PySpark scripts used to generate the data for the analysis is discussed below.
Using a python script, similarly to previous tasks, the bar plot shown to the right was produced, displaying the change in the average gas price per transaction over time. From the graph, we see that the gas price initially started at the global maximum value, then dropping by around 65%. Since then, the average gas price per transaction has slowly tapered down over time.

![image](https://user-images.githubusercontent.com/57494763/168138604-e590c677-a113-41ac-9123-582694d4275d.png)

The plot shown below, produced using matplotlib, shows the average difficulty of each mined block over time. As we can see, the block difficulty skyrockets to a value of approx. 2.5e^15 in September 2017. From there, it drops off again, before again reaching the global peak of around 3.5e^15 in August 2018. We can therefore deduce that the complexity of the blocks has risen significantly over time but does tend to fluctuate. Note also we have a very small number of values at a date of 1970. These were going to be removed, but they correspond to blocks that have an epoch time of 0. These may be missing their values for some reason, but it is interesting to see this anomaly.

![image](https://user-images.githubusercontent.com/57494763/168138671-0f0ab195-f3ff-4793-8795-84bf2638344b.png)

The table produced in python from this data provides the block number, address and gas used of the top 3 most popular services in the Ethereum network. As we can see, comparing the block sizes and gas used for these services with the plot produced in part 2 of this analysis, the gas used per transaction tends to follow the same trend seen in the previous plot. We see though that these services use a much smaller average gas amount per transaction than those seen in the plot in part 2, which may explain why these services are so popular. The max average gas used in this plot is ~〖8e〗^6, whereas the max in the previous plot is 〖3.5〗^15.

![image](https://user-images.githubusercontent.com/57494763/168138728-a9eb70d1-6e76-4b25-b99d-ef1db409f888.png)



