# -*- coding: utf-8 -*-
"""
Created on Wed Oct  3 19:37:40 2018
logic by Archana Anand
@author: Archana Anand
"""

import re

from pyspark import SparkConf, SparkContext

def Func(lines):
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
        			          
    return line_out

if __name__ == "__main__":
   
    conf = SparkConf().setAppName('WordCount')
    spark = SparkContext(conf=conf)
    #lines = spark.read.text(input).rdd.map(lambda r: r[0])
    lines = spark.textFile("input/quotes.list")  
    
    out_list = lines.flatMap(Func) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(lambda x, y: x+y)
    
    out_list = out_list.takeOrdered(2000, key = lambda x: (-1*x[1] , x[0]))
    
    
    rdd_out = spark.parallelize(out_list).coalesce(1)
    dataText = rdd_out.map(lambda x: (str(x[0]) + "\t" + str(x[1])))
    dataText.saveAsTextFile("output")
	
    spark.stop()
