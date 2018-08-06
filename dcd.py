import os
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import pandas as pd

import subprocess



#pyspark imports
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext

















#this method takes path for system potential clone file
#and returns converted csv file to be used by pyspark csv api
def convertAndSaveAsCSV(inputPath):
	soup = ''
	with open(inputPath) as fp:
		soup = BeautifulSoup(fp, 'lxml')

	all_potential_clones = soup.find_all('source')


	total_pcs = len(all_potential_clones)


	df = pd.DataFrame(columns=["filepath", "startline", "endline", "sourceCode"])


	total_pcs = len(all_potential_clones)

	for i in range(0, total_pcs):
		src = all_potential_clones[i].text
		src = src.replace('\n', ' ').replace('\r', '')
	
		df = df.append({'filepath': all_potential_clones[i]['file'], 'startline': all_potential_clones[i]['startline'], 'endline': all_potential_clones[i]['endline'], 'sourceCode': src}, ignore_index=True)



	return df




#this method applies required transformation on the potential clones using txl
#uses pyspark dataframe for distributed computation.
def distributedSourceTransform(row):
	#the txl transformation grammar follows the following format...
	formatted_potential_clones = '<source file="' + row.filepath +  '" startline="' + row.startline +'" endline="'+ row.endline + '"> ' + row.sourceCode + ' </source>'

	#write to a temporary file to be used by the txl parser
	with open('tmp', "w") as fo:
		fo.write(formatted_potential_clones)


	#the required txl transformations...
	p = subprocess.Popen(['/usr/local/bin/txl', '-Dapply', 'NiCad-4.0/txl/java-rename-blind-functions.txl', 'tmp'],
						 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	out, err = p.communicate()

	return (row.filepath, row.startline, row.endline, out)






















#the system file path to detect clone on
potential_clones = 'Datasource/pc.xml'

#loading the potential clones to pandas dataframe
df = convertAndSaveAsCSV(potential_clones)


#get or create spark context
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)


#convert the pandas dataframe to pyspark dataframe
spark_df = sqlContext.createDataFrame(df)



#apply required transformations (as set by user) on the potential clones prior to
#applying near miss clone detection
transformed_spark_rdd = spark_df.rdd.map(distributedSourceTransform)


transformed_spark_df = transformed_spark_rdd.cartesian(transformed_spark_rdd)



print transformed_spark_df.count()






























#spark_df.show()
#print spark_df.count()


