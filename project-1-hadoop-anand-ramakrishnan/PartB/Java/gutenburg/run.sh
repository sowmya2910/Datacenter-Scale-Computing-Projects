#! /bin/bash

rm -r -f output
hadoop fs -rm -r -f input/
hadoop fs -rm -r -f outputWordCount/
hadoop fs -rm -r -f output/
hadoop jar ../wordcount.jar WordCount s3a://csci5253-gutenberg-dataset/ outputWordCount/
hadoop jar ../topfrequency.jar TopKValues outputWordCount/ output/
hadoop fs -copyToLocal output/ 
