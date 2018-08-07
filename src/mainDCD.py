import os
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import pandas as pd

import sys

import subprocess


# pyspark imports
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext


# this method takes path for system potential clone file
# and writes the converted csv to a supplied destination file path
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

        df = df.append({'filepath': all_potential_clones[i]['file'], 'startline': all_potential_clones[i]['startline'],
                        'endline': all_potential_clones[i]['endline'], 'sourceCode': src}, ignore_index=True)

    if saveToFile == True:
        df.to_csv(destinationPath, sep=',')

    return df


def distributedSourceTransform(row):

    # the txl transformation grammar follows the following format...
    formatted_potential_clones = '<source file="' + row.filepath + '" startline="' + row.startline + '" endline="' + row.endline + '"> ' + row.sourceCode + ' </source>'

    # write to a temporary file to be used by the txl parser
    with open('tmp', "w") as fo:
        fo.write(formatted_potential_clones)

    # the required txl transformations...
    p = subprocess.Popen(['/usr/local/bin/txl', '-Dapply', '../NiCad-4.0/txl/java-rename-blind-functions.txl', 'tmp'],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()







    ### this should be the very last operation of
    #this map for source transformation...
    #outputs only resultant trasnsformed source code removing <source/> tags
    transformedSrcCode = BeautifulSoup(out, 'lxml').text
    transformedSrcCode=transformedSrcCode.replace('\n', ' ').replace('\r', '')

    return (row.filepath, row.startline, row.endline, transformedSrcCode)







def main():
    potential_clones = sys.argv[1]
    outDir = sys.argv[2]




    #potential_clones = '../Datasource/pc.xml'
    output_csv = 'csvCodes.csv'
    df = convertAndSaveAsCSV(potential_clones, output_csv, False)

    # spark context
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)
    spark_df = sqlContext.createDataFrame(df)

    transformed_spark_df = spark_df.rdd.map(distributedSourceTransform)

    pysparkdf_transformedClones = transformed_spark_df.toDF()

    pysparkdf_transformedClones.toPandas().to_csv(outDir + '/' +'results.csv')

    







    #pysparkdf_transformedClones.show()

    #print transformed_spark_df.take(5)



if __name__ == "__main__":
	main()




















# spark_df.show()
# print spark_df.count()


