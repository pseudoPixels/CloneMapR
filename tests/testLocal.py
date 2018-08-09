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


















# def main():
#     # the system file path to detect clone on
#     potential_clones = '../Datasource/pc2.xml'
#
#     # loading the potential clones to pandas dataframe
#     df = convertAndSaveAsCSV(potential_clones)
#
#
#     #convert the pandas dataframe to pyspark dataframe
#     pysparkdf_potential_clones = sqlContext.createDataFrame(df).toDF("text")
#
#
#
#     pysparkrdd_preprocessed_potential_clones = pysparkdf_potential_clones.rdd.map(distributedSourceTransform)
#
#     pysparkrdd_preprocessed_potential_clones.take(5)
#
#
#
#
#
#     #df2 = convertAndSaveAsCSV('../Datasource/pc.xml')
#
#
#     #convert the pandas dataframe to pyspark dataframe
#     #train_db = sqlContext.createDataFrame(df2).toDF("text")
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
# #convert the pandas dataframe to pyspark dataframe
# #spark_df = sqlContext.createDataFrame(df)
#
#
# # query = sqlContext.createDataFrame(
# #     ["Hello there  real|y like Spark!",
# # "Hello there",
# # "like Spark!",
# # "Hello like Spark!",
# # "AAAAAAA BBBBBB CCCCCC"
# #     ], "string"
# # ).toDF("text")
# #
# # db2 = sqlContext.createDataFrame([
# #     "AAAAAAA BBBBBB CCCCCC",
# #     "Can anyone suggest an efficient algorithm"
# # ], "string").toDF("text")
#
# #
# # model = Pipeline(stages=[
# #     RegexTokenizer(
# #         pattern="", inputCol="text", outputCol="tokens", minTokenLength=1
# #     ),
# #     NGram(n=3, inputCol="tokens", outputCol="ngrams"),
# #     HashingTF(inputCol="ngrams", outputCol="vectors"),
# #     MinHashLSH(inputCol="vectors", outputCol="lsh")
# # ]).fit(test_db)
# #
# # db_hashed = model.transform(test_db)
# # query_hashed = model.transform(train_db)
# #
# # model.stages[-1].approxSimilarityJoin(db_hashed, query_hashed, 0.75).show()
#
#
#
#
# if __name__ == "__main__":
# 	main()















#
#
# db = sqlContext.createDataFrame([
#     "Hello there I really like Spark",
#     "World there I really ",
#     "Can anyone suggest an efficient algorithm"
# ], "string").toDF("text")
#
#
# model = Pipeline(stages=[
#     RegexTokenizer(
#         pattern="", inputCol="text", outputCol="tokens", minTokenLength=1
#     ),
#     NGram(n=2, inputCol="tokens", outputCol="ngrams"),
#     HashingTF(inputCol="ngrams", outputCol="vectors"),
#     MinHashLSH(inputCol="vectors", outputCol="lsh")
# ]).fit(db)
#
# db_hashed = model.transform(db)
# #query_hashed = model.transform(query)
#
# db_hashed.show()
# #query_hashed.show()
#
# model.stages[-1].approxSimilarityJoin(db_hashed, db_hashed, 1.0).show()









#
#
# db = sqlContext.createDataFrame([
#     (0, "Hello there I really like Spark"),
#     (1, "World there I really "),
#     (2, "Can anyone suggest an efficient algorithm"),
#     (3, "Can anyone suggest an efficient alg"),
#     (4, "Python is a 2000 made-for-TV horror movie directed by Richard"),
#     (5, "Clabaugh. The film features several cult favorite actors, including William"),
#      (6, "Zabka of The Karate Kid fame, Wil Wheaton, Casper Van Dien, Jenny McCarthy,"),
#      (7, "Keith Coogan, Robert Englund (best known for his role as Freddy Krueger in the"),
#      (8, "A Nightmare on Elm Street series of films), Dana Barron, David Bowe, and Sean"),
#      (9, "Whalen. The film concerns a genetically engineered snake, a python, that"),
#      (10, "escapes and unleashes itself on a small town. It includes the classic final"),
#      (11, "girl scenario evident in films like Friday the 13th. It was filmed in Los Angeles,"),
#      (12, "California and Malibu, California. Python was followed by two sequels: Python"),
#      (13, "II (2002) and Boa vs. Python (2004), both also made-for-TV films."),
#
#
#     ]).toDF("id","text")
#
#
#
#
# dbStack = sqlContext.createDataFrame([
#     ("44293378","Background drawable does not work"),
#     ("44293379","Change variable with button in tkinter"),
#     ("44293381","disptach action error, value appearing as nested object"),
#     ("44293382","angular2 - make a variable more variable"),
#     ("44293383","Formatting XML for Twilio Response"),
#     ("44293386","How can I filter objects selected w/ jq based on other values?"),
#     ("44293389","PHP preg_match multiple occurence & multipe uses on same page"),
#     ("44293390","Ruby list all files within a UNC path via WinRM"),
#     ("44293404","Illegal Characters in path when grabbing text from a file?"),
#     ("44293406","MySQL (Get 2 tables in 1 command)"),
#     ("44293407","How can I check whether a given file is FASTA?"),
#     ("44293408","Using Roxygen2 for my utility functions"),
#     ("44293409","Using sed command on remote system"),
#     ("44293412","Place Snackbar at highest z order to avoid from being blocked by AutoCompleteTextView drop down"),
#     ("44293414","Unable to Run Hadoop Jar on Cloudera VM")
#     ]).toDF("id", "text")
#
#
# #dbStack.show()
#
#
#
# stackoverflow_df = sqlContext.read.csv("../Datasource/stackOverFlow_ID_Title_SMALL.csv", header=True).toDF('id', 'text')
#
#
# #stackoverflow_df.show()
#
# #stackoverflow_df.head(10).show()
#
#
# #stack_df = stack_rdd.toDF(['id','text'])
#
# #stackoverflow_df.show()
#
#
# #stackoverflow_df.printSchema()
#
#
#
# model = Pipeline(stages=[
#     RegexTokenizer(
#         pattern="", inputCol="text", outputCol="tokens", minTokenLength=1
#     ),
#     NGram(n=3, inputCol="tokens", outputCol="ngrams"),
#     HashingTF(inputCol="ngrams", outputCol="vectors"),
#     MinHashLSH(inputCol="vectors", outputCol="lsh")
# ]).fit(stackoverflow_df)
#
# db_hashed = model.transform(stackoverflow_df)
#
# #db_hashed.show()
# #query_hashed = model.transform(query)
#
# #db_hashed.show()
# #query_hashed.show()
#
# res = model.stages[-1].approxSimilarityJoin(db_hashed, db_hashed, 0.90).filter("datasetA.id < datasetB.id")
#
# res.show()
#
# #print db_hashed.show()
#
#
#




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



































































