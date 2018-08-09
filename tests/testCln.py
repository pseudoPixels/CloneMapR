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








import sys
import time

def main():
    input_dataset = sys.argv[1]
    output_dir = sys.argv[2]

    start_time = time.time()

    #stackoverflow_df = sqlContext.read.csv("../Datasource/stackOverFlow_ID_Title_SMALL.csv", header=True).toDF('id', 'text')

    stackoverflow_df = sqlContext.read.csv(input_dataset, header=True).toDF('id', 'text')





    # stackoverflow_df.show()

    # stackoverflow_df.head(10).show()


    # stack_df = stack_rdd.toDF(['id','text'])

    # stackoverflow_df.show()


    # stackoverflow_df.printSchema()



    model = Pipeline(stages=[
        RegexTokenizer(
            pattern="", inputCol="text", outputCol="tokens", minTokenLength=1
        ),
        NGram(n=3, inputCol="tokens", outputCol="ngrams"),
        HashingTF(inputCol="ngrams", outputCol="vectors"),
        MinHashLSH(inputCol="vectors", outputCol="lsh") #MinHashLSH(inputCol="vectors", outputCol="lsh", numHashTables=5)
    ]).fit(stackoverflow_df)

    db_hashed = model.transform(stackoverflow_df)

    # db_hashed.show()
    # query_hashed = model.transform(query)

    # db_hashed.show()
    # query_hashed.show()

    #res = model.stages[-1].approxSimilarityJoin(db_hashed, db_hashed, 0.90).filter("datasetA.id < datasetB.id")

    res = model.stages[-1].approxSimilarityJoin(db_hashed, db_hashed, 0.70).filter("distCol > 0")

    #print res

    #print res.count()

    res.show()

    elapsed_time = time.time() - start_time

    print 'Elapsed Time ==> ' , elapsed_time

    #res.toPandas().to_csv(output_dir + '/' + 'results.csv')



if __name__ == "__main__":
	main()



































































