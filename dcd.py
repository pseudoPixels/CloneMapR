import os
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import pandas as pd



#pyspark imports
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext







#this method takes path for system potential clone file
#and writes the converted csv to a supplied destination file path
def convertAndSaveAsCSV(inputPath, destinationPath, saveToFile=True):
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


	if saveToFile == True:	
		df.to_csv(destinationPath, sep=',')

	return df


















potential_clones = 'Datasource/pc.xml'
output_csv = 'csvCodes.csv'
df = convertAndSaveAsCSV(potential_clones, output_csv, False)


#spark context
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
spark_df = sqlContext.createDataFrame(df)

spark_df.show()
#print spark_df.count()


