# Import libraries
from __future__ import print_function
import boto3
import os
import sys
import uuid
import re

# Initializing boto3 client and resource 
s3 = boto3.client('s3')
res = boto3.resource('s3')

# Lambda Handler function to process tweet files and upload to target bucket
def lambda_handler(event, context):
    for record in event['Records']:
	# Declaring target bucket
        target_bucket = 'processedtweetsproject3'
	# Getting input bucket details and reading file from it (as it is uploaded)
        bucket = record['s3']['bucket']['name']
	# Text file name
        key = record['s3']['object']['key'] 
	# Reading the text file 
        response = s3.get_object(Bucket=bucket, Key=key)
        lines = response['Body'].read().decode('utf-8')
	# Declaring stop word list
        stop_word = ['a','about','above','after','again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', 'arent', 'as', 'at', 'be', 'because', 'been',\
	    	          'before', 'being','below', 'between', 'both', 'but','by', 'cant',\
		          'cannot', 'could','couldnt', 'did', 'didnt', 'do','does', 'doesnt', 'doing', 'dont',\
		          'down', 'during', 'each', 'few','for', 'from', 'further', 'had','hadnt',\
		          'has', 'hasnt', 'have','havent', 'having', 'he', 'hed','hell', 'hes', 'her', 'here',
		          'heres', 'hers', 'herself', 'him','himself', 'his', 'how', 'hows','i', 'id',\
		          'ill', 'im','ive', 'if', 'in', 'into','is', 'isnt', 'it', 'its','itself', 'lets', 'me', 'more',
		          'most', 'mustnt', 'my', 'myself','no', 'nor', 'not', 'of','off', 'on',\
		          'once', 'only','or', 'other', 'ought', 'ours','ourselves', 'out', 'over', 'own',\
		          'same', 'shant', 'she', 'shed','shell', 'shes', 'should', 'shouldnt','so', 'some', 'such', 'than',\
		          'that', 'thats', 'the', 'their','theirs', 'them', 'themselves', 'then','there', 'theres', 'these', 'they',\
		          'theyd', 'theyll', 'theyre', 'theyve','this', 'those', 'through', 'to','too', 'under', 'until', 'up',\
		          'very', 'was', 'wasnt', 'we','wed', 'well', 'were', 'weve','werent', 'what', 'whats', 'whens',\
		          'where', 'wheres', 'which', 'while','who', 'whos', 'whom', 'why','whys', 'with', 'wont', 'would',\
		          'wouldnt', 'you', 'youd', 'youll','youre', 'youve', 'your', 'yours','yourself', 'yourselves', 'our','when']
	# Processing - removing non-alphabetic characters, converting to lower-case and removing stop-words
        regex = re.compile('[^a-zA-Z\s+]')
        lines  = regex.sub('', lines)
        regex = re.compile('[\n+]')
        lines  = regex.sub('  ', lines)
        lines = lines.lower()
        lines = lines.strip()
        lines  = re.split(r"\s+",lines)
        line_out = list()
        for line in lines:
            if line not in stop_word:
                if line.strip():
                    line_out.append(line)
	# Converting to string
        string = " ".join(str(x) for x in line_out)
	# Writing to target bucket the processed tweet string, naming the text file the same name it had in input bucket (key)
        res.Bucket(target_bucket).put_object(Key=key, Body=string)
