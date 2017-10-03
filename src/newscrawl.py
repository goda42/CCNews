#Dependencies
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import lit, unix_timestamp
from pyspark.sql.session import SparkSession
from elasticsearch import Elasticsearch, helpers
import os
import re
import json

#Configurations
conf = SparkConf().setAppName('CCReader')
conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", aws_access_key)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", aws_secret_key)
sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "<title>")


#Global Variable Declarations
base_dir = 's3a://commoncrawl/'
countries = dict()
counts = dict()
filelist = []
ccodes3 = dict()
ccodes2 = dict()
geotags = dict()
elastic_ips = []
elastic_login = []

#Iterate through all ccnews files, load in groups of #, send for prelim analysis and saving
def file_loader():
	fileLister()
	newsFile = []
	i=0
	#Load all groups of 5 files
	for j in range(len(filelist)):
		newsFile.append(sc.newAPIHadoopFile(base_dir+filelist[j],"org.apache.hadoop.mapreduce.lib.input.TextInputFormat", "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text"))
		i+=1
		if i==5:
			i=0
			news=newsFile[0].union(newsFile[1].union(newsFile[2].union(newsFile[3].union(newsFile[4]))))
			analyzer(news)
			newsFile = []
	#Handle leftovers
	if i > 0:
		news=newsFile[0]
		for j in range(1,i):
			news=news.union(newsFile[j])
		analzyer(news)
	
#Do mappings, check for country names, send records for saving to elastic
def analyzer(data):
	records = data.map(titleMap)
	records = records.filter(lambda x: x).persist()
	for country in countries:
	        news = records.filter(lambda x: newsFilter(x,country))
		news = news.map(lambda x: countryMap(x,country))
		news = news.map(jsonMap)
		news.foreachPartition(esSave)
	records.unpersist()

#Gets list of news CC warc files
def fileLister():
	f = open('./dep/ccnewsfiles.txt')
	#f = open('./dep/temp.txt')
	for line in f:
		if line != '':
			filelist.append(line.strip())

#Gets all country names and aliases from file
def country_loader():
	cfile = open('./dep/country-list.txt')
	for line in cfile:
		temp = line.strip().split(',')
		temp = filter(None, temp)
		if temp != ['']:
			countries[int(temp[0])]=temp[5:]
			geotags[int(temp[0])]=[float(temp[3]),float(temp[4])]
			ccodes3[int(temp[0])]=temp[1]
			ccodes2[int(temp[0])]=temp[2]			

#Custom Map for getting news article title and timestamp
def titleMap(s):
	try:
		m = re.search('(.*)</title>',s[1])
		n = re.search('(\d{4}\D\d{2}\D\d{2}T\d{2}:\d{2}:\d{2})',s[1])
		if m != None and n != None:
			return (s[0],m.group(0)[:-8],n.group(0))	
	except:
		print "Bad Data"
		return [None, None, None]

#Custom Filter for finding articles that mention country or aliases
def newsFilter(s, filt):
	title = s[1].split(" ")
	for term in countries[filt]:
		if term in title or term.upper() in title or term.lower() in title:
			return True
	return False

#Add country names, codes, and geotag to RDD
def countryMap(s,country):
	nation = list(s)
	nation.append(countries[country][0])
	nation.append(ccodes2[country])
	nation.append(ccodes3[country])
	nation.append(geotags[country])
	return nation

#Map values in RDD into JSON for export to elastic
def jsonMap(s):
	j = {"_index":"test18","_type":"articles","date":s[2],'title':s[1],'country':s[3],'ccode2':s[4],'ccode3':s[5],}#'geo':s[6]}
	return [s[0],j]

#Save data on elasti cluster
def esSave(s):
        es = Elasticsearch(elastic_ips,http_auth=(elastic_login[0], elastic_login[1]),port=9200,)
        action = []
        for i in s:
                action.append(i[1])
                if len(action) == 500:
                        helpers.bulk(es,action,request_timeout=30)
                        action=[]
        if len(action) > 0:
                helpers.bulk(es,action)

#Load IPs and login information on elastic cluster from file
def elastic_load():
	f = open('./dep/elastic.txt')
	for i in range(4):
		elastic_ips.append(f.readline().strip())
	for i in range(2):
		elastic_login.append(f.readline().strip())

#Main
def main():
	elastic_load()
	country_loader()
	file_loader()

if __name__ == "__main__":
	main()
