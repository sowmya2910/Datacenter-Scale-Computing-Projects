# -*- coding: utf-8 -*-
"""
Created on Sat Oct  6 15:32:00 2018
logic thought by Archana 
@author: Archana Anand
"""




from pyspark.sql import SparkSession
from operator import add


if __name__ == "__main__":
    spark = SparkSession \
            .builder \
            .appName("myApp") \
            .getOrCreate()
            
    

    spark_conf = spark.sparkContext     
            
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
            
    df_review = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://student:student@ec2-54-210-44-189.compute-1.amazonaws.com/test.reviews").load()
    df_1 = df_review.filter(df_review.overall == 1).select(["reviewText"])
    df_2 = df_review.filter(df_review.overall == 2).select(["reviewText"])
    df_3 = df_review.filter(df_review.overall == 3).select(["reviewText"])
    df_4 = df_review.filter(df_review.overall == 4).select(["reviewText"])
    df_5 = df_review.filter(df_review.overall == 5).select(["reviewText"])
    
  
    new_string = df_1.rdd.map(lambda l: ''.join(filter(lambda x: x.isalpha() | x.isspace(), l.reviewText.lower())))
    words = new_string.flatMap(lambda l: l.split()).filter(lambda x: x not in stop_word).filter(lambda x: len(x)>0).map(lambda y: (y, 1)).reduceByKey(add).sortByKey()
    out_1 = words.takeOrdered(500, lambda x: -x[1])
    rdd_out = spark_conf.parallelize(out_1).coalesce(1)
    dataText = rdd_out.map(lambda x: (str(x[0]) + "\t" + str(x[1])))
    dataText.saveAsTextFile("Bucket1")
    
    new_string = df_2.rdd.map(lambda l: ''.join(filter(lambda x: x.isalpha() | x.isspace(), l.reviewText.lower())))
    words = new_string.flatMap(lambda l: l.split()).filter(lambda x: x not in stop_word).filter(lambda x: len(x)>0).map(lambda y: (y, 1)).reduceByKey(add).sortByKey()
    out_1 = words.takeOrdered(500, lambda x: -x[1])
    rdd_out = spark_conf.parallelize(out_1).coalesce(1)
    dataText = rdd_out.map(lambda x: (str(x[0]) + "\t" + str(x[1])))
    dataText.saveAsTextFile("Bucket2")
    
    new_string = df_3.rdd.map(lambda l: ''.join(filter(lambda x: x.isalpha() | x.isspace(), l.reviewText.lower())))
    words = new_string.flatMap(lambda l: l.split()).filter(lambda x: x not in stop_word).filter(lambda x: len(x)>0).map(lambda y: (y, 1)).reduceByKey(add).sortByKey()
    out_1 = words.takeOrdered(500, lambda x: -x[1])
    rdd_out = spark_conf.parallelize(out_1).coalesce(1)
    dataText = rdd_out.map(lambda x: (str(x[0]) + "\t" + str(x[1])))
    dataText.saveAsTextFile("Bucket3")
    
    new_string = df_4.rdd.map(lambda l: ''.join(filter(lambda x: x.isalpha() | x.isspace(), l.reviewText.lower())))
    words = new_string.flatMap(lambda l: l.split()).filter(lambda x: x not in stop_word).filter(lambda x: len(x)>0).map(lambda y: (y, 1)).reduceByKey(add).sortByKey()
    out_1 = words.takeOrdered(500, lambda x: -x[1])
    rdd_out = spark_conf.parallelize(out_1).coalesce(1)
    dataText = rdd_out.map(lambda x: (str(x[0]) + "\t" + str(x[1])))
    dataText.saveAsTextFile("Bucket4")
    
    new_string = df_5.rdd.map(lambda l: ''.join(filter(lambda x: x.isalpha() | x.isspace(), l.reviewText.lower())))
    words = new_string.flatMap(lambda l: l.split()).filter(lambda x: x not in stop_word).filter(lambda x: len(x)>0).map(lambda y: (y, 1)).reduceByKey(add).sortByKey()
    out_1 = words.takeOrdered(500, lambda x: -x[1])
    rdd_out = spark_conf.parallelize(out_1).coalesce(1)
    dataText = rdd_out.map(lambda x: (str(x[0]) + "\t" + str(x[1])))
    dataText.saveAsTextFile("Bucket5")
    
    
    spark.stop()
