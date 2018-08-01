import os
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import pandas as pd

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

    with open('tmp', "w") as fo:
        fo.write(row.sourceCode)

    p = subprocess.Popen(['/usr/local/bin/txl', '-Dapply', 'txl_features/java/normalizeLiteralsToDefault.txl', 'tmp'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()

    return (row.filepath, row.startline, row.endline, out)


# potential_clones = 'Datasource/pc.xml'
# output_csv = 'csvCodes.csv'
# df = convertAndSaveAsCSV(potential_clones, output_csv, False)
#
# # spark context
# sc = SparkContext.getOrCreate()
# sqlContext = SQLContext(sc)
# spark_df = sqlContext.createDataFrame(df)
#
# transformed_spark_df = spark_df.rdd.map(distributedSourceTransform)
#
# print transformed_spark_df.take(5)




def txlTest():
    p = subprocess.Popen(['/usr/local/bin/txl', '/home/gom766/Documents/My_Research_Projects/dcd/NiCad-4.0/txl/java-rename-blind-functions.txl', 'tmp2'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()

    return err




print txlTest()




















# spark_df.show()
# print spark_df.count()


