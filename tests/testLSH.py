import os
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import pandas as pd

import subprocess




from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, NGram, HashingTF, MinHashLSH


from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext


#get or create spark context
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)


# this method takes path for system potential clone file
# and returns converted csv file to be used by pyspark csv api
def convertAndSaveAsCSV(inputPath):
    soup = ''
    with open(inputPath) as fp:
        soup = BeautifulSoup(fp, 'lxml')

    all_potential_clones = soup.find_all('source')

    total_pcs = len(all_potential_clones)

    df = pd.DataFrame(columns=["sourceCode"])

    total_pcs = len(all_potential_clones)

    for i in range(0, total_pcs):
        src = all_potential_clones[i].text
        src = src.replace('\n', ' ').replace('\r', '')

        df = df.append({'filepath': all_potential_clones[i]['file'], 'startline': all_potential_clones[i]['startline'],
                        'endline': all_potential_clones[i]['endline'], 'sourceCode': src}, ignore_index=True)

    if saveToFile == True:
        df.to_csv(destinationPath, sep=',')

    return df






# this method applies required transformation on the potential clones using txl
# uses pyspark dataframe for distributed computation.
def distributedSourceTransform(row):
    # the txl transformation grammar follows the following format...
    formatted_potential_clones = '<source file="' + row.filepath + '" startline="' + row.startline + '" endline="' + row.endline + '"> ' + row.sourceCode + ' </source>'

    # write to a temporary file to be used by the txl parser
    with open('tmp', "w") as fo:
        fo.write(formatted_potential_clones)

    # the required txl transformations...
    p = subprocess.Popen(['/usr/local/bin/txl', '-Dapply', 'NiCad-4.0/txl/java-rename-blind-functions.txl', 'tmp'],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()

    return (row.filepath, row.startline, row.endline, out)


















def main():
    # the system file path to detect clone on
    potential_clones = '../Datasource/pc2.xml'

    # loading the potential clones to pandas dataframe
    df = convertAndSaveAsCSV(potential_clones)


    #convert the pandas dataframe to pyspark dataframe
    pysparkdf_potential_clones = sqlContext.createDataFrame(df).toDF("text")



    pysparkrdd_preprocessed_potential_clones = pysparkdf_potential_clones.rdd.map(distributedSourceTransform)

    pysparkrdd_preprocessed_potential_clones.take(5)





    #df2 = convertAndSaveAsCSV('../Datasource/pc.xml')


    #convert the pandas dataframe to pyspark dataframe
    #train_db = sqlContext.createDataFrame(df2).toDF("text")

















#convert the pandas dataframe to pyspark dataframe
#spark_df = sqlContext.createDataFrame(df)


# query = sqlContext.createDataFrame(
#     ["Hello there  real|y like Spark!",
# "Hello there",
# "like Spark!",
# "Hello like Spark!",
# "AAAAAAA BBBBBB CCCCCC"
#     ], "string"
# ).toDF("text")
#
# db2 = sqlContext.createDataFrame([
#     "AAAAAAA BBBBBB CCCCCC",
#     "Can anyone suggest an efficient algorithm"
# ], "string").toDF("text")

#
# model = Pipeline(stages=[
#     RegexTokenizer(
#         pattern="", inputCol="text", outputCol="tokens", minTokenLength=1
#     ),
#     NGram(n=3, inputCol="tokens", outputCol="ngrams"),
#     HashingTF(inputCol="ngrams", outputCol="vectors"),
#     MinHashLSH(inputCol="vectors", outputCol="lsh")
# ]).fit(test_db)
#
# db_hashed = model.transform(test_db)
# query_hashed = model.transform(train_db)
#
# model.stages[-1].approxSimilarityJoin(db_hashed, query_hashed, 0.75).show()




if __name__ == "__main__":
	main()