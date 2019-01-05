#! /bin/bash

rm -r -f Bucket1
rm -r -f Bucket2
rm -r -f Bucket3
rm -r -f Bucket4
rm -r -f Bucket5
hadoop fs -rm -r -f Bucket1
hadoop fs -rm -r -f Bucket2
hadoop fs -rm -r -f Bucket3
hadoop fs -rm -r -f Bucket4
hadoop fs -rm -r -f Bucket5
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.0 partb.py
hadoop fs -copyToLocal Bucket1
hadoop fs -copyToLocal Bucket2
hadoop fs -copyToLocal Bucket3
hadoop fs -copyToLocal Bucket4
hadoop fs -copyToLocal Bucket5


