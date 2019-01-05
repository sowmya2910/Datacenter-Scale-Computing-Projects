#! /bin/bash

rm -rf output.txt
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.0 parta.py
