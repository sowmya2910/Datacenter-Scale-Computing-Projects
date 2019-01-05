# -*- coding: utf-8 -*-
"""
Created on Sat Oct  6 11:38:57 2018

@author: Archana Anand
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from collections import defaultdict

if __name__ == "__main__":
    spark = SparkSession \
            .builder \
            .appName("myApp") \
            .getOrCreate()
            
    df_review = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://student:student@ec2-54-210-44-189.compute-1.amazonaws.com/test.reviews").load()
    df_metadata = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://student:student@ec2-54-210-44-189.compute-1.amazonaws.com/test.metadata").load()
    
    joined_df = df_review.groupBy(df_review.asin).agg(func.mean("overall").alias("overall"),func.count(func.lit(1)).alias("num"))
    joined_df = joined_df.filter(joined_df.num>99)
    joined_df = joined_df.join(df_metadata, joined_df.asin == df_metadata.asin, 'inner').drop(df_metadata.asin)
    
    joined_df = joined_df.orderBy(joined_df.overall.desc()).coalesce(1).collect()
    
    d = defaultdict(list)
    
    a_movie = "Movies & TV"
    a_CDsVinyl = "CDs & Vinyl"
    a_VideoGames = "Video Games"
    a_ToysGames = "Toys & Games"
    movie = ""
    cd = ""
    vg = ""
    tg = ""
    extra = ""
    count = 0;
    for items in joined_df:
        count = count+1
        cat = items.categories
        if items.title is None:
            title = "null"
        else:
            title = items.title
        if a_CDsVinyl in cat[0] and cd == "":
            cd = a_CDsVinyl + "\t" + title + "\t" + str(items.num) + "\t" + str(items.overall) + "\n"            
        if a_movie in cat[0] and movie == "":
            movie = a_movie + "\t" + title + "\t" + str(items.num) + "\t" + str(items.overall) + "\n"            
        if a_VideoGames in cat[0] and vg == "":
            vg = a_VideoGames + "\t" + title + "\t" + str(items.num) + "\t" + str(items.overall) + "\n"            
        if a_ToysGames in cat[0] and tg == "":
            tg = a_ToysGames + "\t" + title + "\t" + str(items.num) + "\t" + str(items.overall) + "\n"            
        if movie != "" and cd != "" and vg != "" and tg != "": 
            d[a_CDsVinyl].append(cd)
            d[a_movie].append(movie)
            d[a_VideoGames].append(vg)
            d[a_ToysGames].append(tg)
            break

    flag = 0
    for items in joined_df:
        cat = items.categories
        if items.title is None:
            title = "null"
        else:
            title = items.title
        if a_CDsVinyl in cat[0]:
            cd = a_CDsVinyl + "\t" + title + "\t" + str(items.num) + "\t" + str(items.overall) + "\n" 
            if cd not in d[a_CDsVinyl]:
                d[a_CDsVinyl].append(cd)
                flag = 1                
        if a_movie in cat[0]:
            movie = a_movie + "\t" + title + "\t" + str(items.num) + "\t" + str(items.overall) + "\n"
            if movie not in d[a_movie]:
                d[a_movie].append(movie)
                flag = 1             
        if a_VideoGames in cat[0]:
            vg = a_VideoGames + "\t" + title + "\t" + str(items.num) + "\t" + str(items.overall) + "\n"
            if vg not in d[a_VideoGames]:
                d[a_VideoGames].append(vg)
                flag = 1             
        if a_ToysGames in cat[0]:
            tg = a_ToysGames + "\t" + title + "\t" + str(items.num) + "\t" + str(items.overall) + "\n"
            if tg not in d[a_ToysGames]:
                d[a_ToysGames].append(tg)
                flag = 1
        if flag ==1:
            break;
    
    
    with open("output.txt", "w") as f:
        for items in d[a_CDsVinyl]:            
            f.write(items)
        for items in d[a_movie]:            
            f.write(items)
        for items in d[a_VideoGames]:            
            f.write(items)
        for items in d[a_ToysGames]:            
            f.write(items)
    
    spark.stop()