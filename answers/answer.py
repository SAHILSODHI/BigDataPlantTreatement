import csv
import os
import sys
# Spark imports
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
# Dask imports
import dask.bag as db
import dask.dataframe as df  # you can use Dask bags or dataframes
from csv import reader

'''
INTRODUCTION

The goal of this assignment is to implement a basic analysis of textual 
data using Apache Spark (http://spark.apache.org) and 
Dask (https://dask.org). 
'''

'''
DATASET

We will study a dataset provided by the city of Montreal that contains 
the list of trees treated against the emerald ash borer 
(https://en.wikipedia.org/wiki/Emerald_ash_borer). The dataset is 
described at 
http://donnees.ville.montreal.qc.ca/dataset/frenes-publics-proteges-injection-agrile-du-frene 
(use Google translate to translate from French to English). 

We will use the 2015 and 2016 data sets available in directory `data`.
'''

'''
HELPER FUNCTIONS

'''


# Initialize a spark session.
def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark


# Useful functions to print RDDs and Dataframes.
def toCSVLineRDD(rdd):
    '''
    This function convert an RDD or a DataFrame into a CSV string
    '''
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row])) \
        .reduce(lambda x, y: os.linesep.join([x, y]))
    return a + os.linesep


def toCSVLine(data):
    '''
    Convert an RDD or a DataFrame into a CSV string
    '''
    if isinstance(data, RDD):
        return toCSVLineRDD(data)
    elif isinstance(data, DataFrame):
        return toCSVLineRDD(data.rdd)
    return None

'''
SPARK RDD IMPLEMENTATION

'''

def count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_rdd.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    sc = spark.sparkContext.textFile(filename)
    header = sc.first()
    rows = sc.filter(lambda x: x != header)
    return rows.count()

# RDD functions
def parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_rdd.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    sc = spark.sparkContext.textFile(filename)
    header = sc.first()
    rows = sc.filter(lambda x: x != header)
    lines = rows.mapPartitions(lambda x: csv.reader(x), 10000)
    nom_parc = lines.map(lambda x: x[6])
    not_null_nom_parc = nom_parc.filter(lambda x: x is not '')
    return not_null_nom_parc.count()

def uniq_parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of unique parks where
    trees were treated. The list must be ordered alphabetically. Every element
    in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_rdd.py
    Note: The return value should be a CSV string
    '''

    spark = init_spark()
    sc = spark.sparkContext.textFile(filename)
    header = sc.first()
    rows = sc.filter(lambda x: x != header)
    lines = rows.mapPartitions(lambda x: csv.reader(x), 10000)
    nom_parc = lines.map(lambda x: x[6])
    not_null_nom_parc = nom_parc.filter(lambda x: x is not '')
    unique_parc_nom = not_null_nom_parc.distinct().collect()
    unique_parc_nom.sort()
    result_str = ""
    for elem in unique_parc_nom:
        result_str = result_str + elem + "\n"
    return result_str

def uniq_parks_counts_rdd(filename):
    '''
    Write a Python script using RDDs that counts the number of trees treated in
    each park and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts_rdd.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    spark = init_spark()
    sc = spark.sparkContext.textFile(filename)
    header = sc.first()
    rows = sc.filter(lambda x: x != header)
    lines = rows.mapPartitions(lambda x: csv.reader(x), 10000)
    nom_parc = lines.map(lambda x: x[6]).filter(lambda x: x is not '').map(lambda word: (word, 1)).reduceByKey(
        lambda a, b: a + b)
    not_null_nom_parc_string = toCSVLineRDD(nom_parc.sortByKey())
    return not_null_nom_parc_string

def frequent_parks_count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of the 10 parks with
    the highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar
    number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    spark = init_spark()
    sc = spark.sparkContext.textFile(filename)
    header = sc.first()
    rows = sc.filter(lambda x: x != header)
    lines = rows.mapPartitions(lambda x: csv.reader(x), 10000)
    nom_parc = lines.map(lambda x: x[6]).filter(lambda x: x is not '').map(lambda word: (word, 1)).reduceByKey(
        lambda a, b: a + b)
    nom_parc_sort = toCSVLineRDD(nom_parc.sortByKey().map(lambda x:(x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0])))
    final_string = ""
    for i in range(10):
        final_string = final_string +nom_parc_sort.splitlines()[i] + "\n"
    return final_string

def intersection_rdd(filename1, filename2):
    '''
    Write a Python script using RDDs that prints the alphabetically sorted list
    of parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    spark = init_spark()
    sc1 = spark.sparkContext.textFile(filename1)
    header1 = sc1.first()
    rows1 = sc1.filter(lambda x: x != header1)
    lines1 = rows1.mapPartitions(lambda x: csv.reader(x), 10000)
    nom_parc1 = lines1.map(lambda x: x[6])
    not_null_nom_parc1 = nom_parc1.filter(lambda x: x is not '').distinct()
    sc2 = spark.sparkContext.textFile(filename2)
    header2 = sc2.first()
    rows2 = sc2.filter(lambda x: x != header2)
    lines2 = rows2.mapPartitions(lambda x: csv.reader(x), 10000)
    nom_parc2 = lines2.map(lambda x: x[6])
    not_null_nom_parc2 = nom_parc2.filter(lambda x: x is not '').distinct()
    net_not_null_parc = not_null_nom_parc1.intersection(not_null_nom_parc2).collect()
    net_not_null_parc.sort()
    result_str = ""
    for elem in net_not_null_parc:
        result_str = result_str + elem + "\n"
    return result_str


'''
SPARK DATAFRAME IMPLEMENTATION

'''


# DataFrame functions

def count_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_df.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    sc = spark._create_shell_session()
    df = sc.read.csv(filename, header=True, mode="DROPMALFORMED")
    return df.count()

def parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_df.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    sc = spark._create_shell_session()
    df = sc.read.csv(filename, header=True, mode="DROPMALFORMED")
    cf = df.filter(df.Nom_parc.isNotNull()).count()
    return cf

def uniq_parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_df.py
    Note: The return value should be a CSV string
    '''

    spark = init_spark()
    sc = spark._create_shell_session()
    df = sc.read.csv(filename, header=True, mode="DROPMALFORMED")
    cf = df.filter(df.Nom_parc.isNotNull())
    ef = cf.select("Nom_parc").distinct().orderBy("Nom_parc")
    gf = toCSVLine(ef)
    return gf

def uniq_parks_counts_df(filename):
    '''
    Write a Python script using DataFrames that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_df.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    spark = init_spark()
    sc = spark._create_shell_session()
    df = sc.read.csv(filename, header=True, mode="DROPMALFORMED")
    cf = df.filter(df.Nom_parc.isNotNull()).orderBy("Nom_parc")
    ef = toCSVLine(cf.groupBy("Nom_parc").count())
    return ef

def frequent_parks_count_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    spark = init_spark()
    sc = spark._create_shell_session()
    df = sc.read.csv(filename, header=True, mode="DROPMALFORMED")
    cf = df.filter(df.Nom_parc.isNotNull()).orderBy("Nom_parc")
    ef = toCSVLine(cf.groupBy("Nom_parc").count().orderBy("Count").sort(desc("Count")).limit(10))
    return ef

def intersection_df(filename1, filename2):
    '''
    Write a Python script using DataFrames that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    spark = init_spark()
    sc = spark._create_shell_session()
    df = sc.read.csv(filename1, header=True, mode="DROPMALFORMED")
    cf = df.filter(df.Nom_parc.isNotNull())
    ef = cf.select("Nom_parc").distinct().orderBy("Nom_parc")

    sc2 = spark._create_shell_session()
    df2 = sc2.read.csv(filename2, header=True, mode="DROPMALFORMED")
    cf2 = df2.filter(df2.Nom_parc.isNotNull())
    ef2 = cf2.select("Nom_parc").distinct().orderBy("Nom_parc")
    final_df = toCSVLine(ef.join(ef2, ['Nom_parc']))
    return final_df

'''
DASK IMPLEMENTATION (bonus)

'''


# Dask functions

def count_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_dask.py
    Note: The return value should be an integer
    '''

    dd = df.read_csv(filename, dtype={'Nom_parc': 'object'})
    return dd.__len__()

def parks_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_dask.py
    Note: The return value should be an integer
    '''

    dd = df.read_csv(filename, dtype={'Nom_parc': 'object'})
    return dd.Nom_parc.dropna().__len__()

def uniq_parks_dask(filename):
    '''
    Write a Python script using Dask that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_dask.py
    Note: The return value should be a CSV string
    '''

    dd = df.read_csv(filename, dtype={'Nom_parc': 'object'}).compute()
    ed = dd.Nom_parc.dropna().to_frame().sort_values(['Nom_parc'])
    fd = ed['Nom_parc'].unique().tolist()
    str1 = "\n".join(str(e) for e in fd) + "\n"
    return str1

def uniq_parks_counts_dask(filename):
    '''
    Write a Python script using Dask that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_dask.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    def print_result(cd):
        result = "\n".join((tree + "," + str(ed.loc[tree].values[0])) for tree in cd) + "\n"
        return result

    dd = df.read_csv(filename, dtype={'Nom_parc': 'object'}).compute()
    ed = dd.Nom_parc.value_counts().to_frame()
    cd = sorted(ed.index)
    fd = print_result(cd)
    return fd

def frequent_parks_count_dask(filename):
    '''
    Write a Python script using Dask that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    def print_result(cd):
        result = "\n".join((tree + "," + str(ed.loc[tree].values[0])) for tree in ld) + "\n"
        return result

    dd = df.read_csv(filename, dtype={'Nom_parc': 'object'}).compute()
    ed = dd.Nom_parc.value_counts().to_frame()
    ld = ed.index.tolist()[:10]
    fd = print_result(ld)
    return fd

def intersection_dask(filename1, filename2):
    '''
    Write a Python script using Dask that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    def print_result(cd):
        result = "\n".join(str(tree[0]) for tree in Intersection) + "\n"
        return result

    dd = df.read_csv(filename1, dtype={'Nom_parc': 'object'})
    ed = dd.Nom_parc.dropna().unique().to_frame().compute()
    dd2 = df.read_csv(filename2, dtype={'No_Civiq': 'object', 'Nom_parc': 'object'})
    ed2 = dd2.Nom_parc.dropna().unique().to_frame().compute()
    Intersection = ed.merge(ed2, on=['Nom_parc']).values
    res = print_result(Intersection)
    return res


# Python answer functions
def count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees (non-header lines) in
    the data file passed as first argument.
    Test file: tests/test_count.py
    Note: The return value should be an integer
    '''
    with open(filename,"r") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        csv_file.readline()
        line_count = 0
        for row in csv_reader:
            line_count += 1
    return line_count

def parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks.py
    Note: The return value should be an integer
    '''

    with open(filename,"r") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        csv_file.readline()
        line_count = 0
        for row in csv_reader:
            if (row[6] != ''):
                line_count += 1
    return line_count

def uniq_parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of unique parks where trees
    were treated. The list must be ordered alphabetically. Every element in the list must be printed on
    a new line.
    Test file: tests/test_uniq_parks.py
    Note: The return value should be a string with one park name per line
    '''

    with open(filename,"r") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        csv_file.readline()
        parkList = sorted(csv_reader, reverse=False, key=lambda row: row[6])
        resultString = ""
        unique_parc = []
        for row in parkList:
            if (row[6] != ''):
                if (row[6] not in unique_parc):
                    unique_parc.append(row[6])
                    resultString = resultString + row[6] + "\n"
    return resultString


def uniq_parks_counts(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that counts the number of trees treated in each park
    and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    def get_result(result_dict):
        result = "\n".join(tree + ',' + str(result_dict[tree]) for tree in sorted(result_dict)) + "\n"
        return result

    with open(filename, "r") as csv_file:
        csv_reader = csv.DictReader(csv_file, restval=None, dialect='excel', delimiter=',')
        result1 = set()
        result_dict = {}
        for row in csv_reader:
            if row['Nom_parc'] != '':
                count = result_dict.setdefault(row['Nom_parc'], 0)
                result_dict[row['Nom_parc']] = count + 1

        result = get_result(result_dict)
        return result

def frequent_parks_count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of the 10 parks with the
    highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar number.
    Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    def get_result(result_dict):
        result = "\n".join(tree + ',' + str(result_dict[tree]) for tree in parkList) + "\n"
        return result

    with open(filename, "r") as csv_file:
        csv_reader = csv.DictReader(csv_file, restval=None, dialect='excel', delimiter=',')
        result1 = set()
        result_dict = {}
        for row in csv_reader:
            if row['Nom_parc'] != '':
                nom_count = result_dict.setdefault(row['Nom_parc'],0)
                result_dict[row['Nom_parc']] = nom_count + 1
        parkList = sorted(result_dict, reverse=True, key=result_dict.__getitem__)
        parkList = parkList[0:10]
        result = get_result(result_dict)
        return result

def intersection(filename1, filename2):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the alphabetically sorted list of
    parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    def intersection(result3):
        result = "\n".join(str(tree) for tree in result3)+"\n"
        return result

    with open(filename1, "r") as csv_file:
        csv_reader = csv.DictReader(csv_file, restval=None, dialect='excel', delimiter=',')
        result1 = set()
        for row in csv_reader:
            if row['Nom_parc'] != '':
                result1.add(row['Nom_parc'])
    with open(filename2, "r") as csv_file2:
        csv_reader2 = csv.DictReader(csv_file2, restval=None, dialect='excel', delimiter=',')
        result2 = set()
        for row in csv_reader2:
            if row['Nom_parc'] != '':
                result2.add(row['Nom_parc'])
    result3 = sorted(result1.intersection(result2))
    result = intersection(result3)
    return result
