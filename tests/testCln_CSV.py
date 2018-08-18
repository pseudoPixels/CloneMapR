import os
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import pandas as pd

import sys

import subprocess
import time

# pyspark imports
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext




from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, NGram, HashingTF, MinHashLSH
from pyspark.sql.functions import col



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

    start_time = time.time()




    #
    #
    # potential_clones = '../Datasource/pc.xml'
    # output_csv = 'csvCodes2.csv'
    # df = convertAndSaveAsCSV(potential_clones, output_csv, True)





    # spark context
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)
    #spark_df = sqlContext.createDataFrame(df)
    spark_df = sqlContext.read.csv(potential_clones, header=True)

    #transformed_spark_df = spark_df.rdd.map(distributedSourceTransform)

    #pysparkdf_transformedClones = transformed_spark_df.toDF(['id','filepath', 'startline', 'endline', 'source'])

    pysparkdf_transformedClones = spark_df.toDF('id','filepath', 'startline', 'endline', 'source')

    #pysparkdf_transformedClones.show()
    #pysparkdf_transformedClones.show()





    model = Pipeline(stages=[
        RegexTokenizer(
            pattern=" ", inputCol="source", outputCol="tokens", minTokenLength=1
        ),
        NGram(n=3, inputCol="tokens", outputCol="ngrams"),
        HashingTF(inputCol="ngrams", outputCol="vectors", numFeatures=262144),#numFeatures=262144
        MinHashLSH(inputCol="vectors", outputCol="lsh", numHashTables=5) #MinHashLSH(inputCol="vectors", outputCol="lsh", numHashTables=5)
    ]).fit(pysparkdf_transformedClones)

    hashed_clones = model.transform(pysparkdf_transformedClones)

    clone_pairs = model.stages[-1].approxSimilarityJoin(hashed_clones, hashed_clones, 0.70).filter("distCol > 0").select(col('datasetA.filepath').alias('filepath1'), col('datasetA.startline').alias('startline1'), col('datasetA.endline').alias('endline1'),col('datasetB.filepath').alias('filepath2'), col('datasetB.startline').alias('startline2'), col('datasetB.endline').alias('endline2'))

    clone_pairs.show()

    clone_pairs.write.csv(outDir+'/detectedClones.csv')


    #detectedClones = clone_pairs.select('distCol')

    #clone_pairs.show()
    #detectedClones.write.save(outDir + '/detectedClones.csv')









    elapsed_time = time.time() - start_time

    print 'Elapsed Time ==> ' , elapsed_time














    #pysparkdf_transformedClones.show()

    #print transformed_spark_df.take(5)



if __name__ == "__main__":
	main()




















# spark_df.show()
# print spark_df.count()












# stackoverflow_df = sqlContext.read.csv("../Datasource/stackOverFlow_ID_Title_SMALL.csv", header=True).toDF('id', 'text')

# stackoverflow_df = sqlContext.read.csv(input_dataset, header=True).toDF('id', 'text')




#
# # stackoverflow_df.show()
#
# # stackoverflow_df.head(10).show()
#
#
# # stack_df = stack_rdd.toDF(['id','text'])
#
# # stackoverflow_df.show()
#
#
# # stackoverflow_df.printSchema()
#
#
#
# model = Pipeline(stages=[
#     RegexTokenizer(
#         pattern="", inputCol="text", outputCol="tokens", minTokenLength=1
#     ),
#     NGram(n=3, inputCol="tokens", outputCol="ngrams"),
#     HashingTF(inputCol="ngrams", outputCol="vectors"),
#     MinHashLSH(inputCol="vectors", outputCol="lsh") #MinHashLSH(inputCol="vectors", outputCol="lsh", numHashTables=5)
# ]).fit(stackoverflow_df)
#
# db_hashed = model.transform(stackoverflow_df)
#
# # db_hashed.show()
# # query_hashed = model.transform(query)
#
# # db_hashed.show()
# # query_hashed.show()
#
# #res = model.stages[-1].approxSimilarityJoin(db_hashed, db_hashed, 0.90).filter("datasetA.id < datasetB.id")
#
# res = model.stages[-1].approxSimilarityJoin(db_hashed, db_hashed, 0.70).filter("distCol > 0")
#
# #print res
#
# #print res.count()
#
# res.show()
























































