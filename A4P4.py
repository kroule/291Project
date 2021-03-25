#PART (Application) 4
#Drop the index, again for all databases, created in Task 10 above and write a Python application (A4P4.py) that embeds SQL statements for executing the following queries as per the requirements in the tasks below
#Write a Python application (A4P4.py) that embeds Sql statements for executing the following query as per the requirements in the task below.
#   Q5: Considering the parts that that exist in Parts, find the quantity of parts that are not used in any other part, your query must use EXISTS.
#   Q6: Considering the parts that that exist in Parts, find the quantity of parts that are not used in any other part, your query must use NOT IN.
#Tasks:
#   Run Q5 100 times and record the average query time required for the database to answer Q5 queries using each version of table Parts.
#   Run Q6 100 times and record the average query time required for the database to answer Q6 queries using each version of table Parts.
#   Compare and contrast the values obtained from Tasks 13 and 14.
#   Define and create one INDEX with the goal of optimizing query Q6. Take note of its space cost.
#   Repeat Task 14, i.e., Run Q6 100 times and record the average query time required for the database to answer Q6 queries using each version of table Parts.
#   Compare and contrast the results obtained for Task 14 with those from Task 17. Discuss in which cases the space cost of the index seemed like a worthwhile "investment.”


import sqlite3
import time

connection = None
cursor = None

DATABASE_INFO = [('./A4v100.db', 100), 
                ('./A4v1k.db',1000),
                ('./A4v10k.db', 10000),
                ('./A4v100k.db', 100000),
                ('./A4v1M.db', 1000000)]
countries = []
upc_codes = []




def runQuery(): 
    global connection, cursor

    Query1 = """SELECT COUNT(partNumber) FROM Parts WHERE NOT EXISTS (SELECT needsPart FROM Parts)"""

    Query2 = """SELECT COUNT(partNumber) FROM Parts WHERE partNumber NOT IN (needsPart)"""
    dropIndex1 = ("DROP INDEX IF EXISTS idxMadeIN;")
    dropIndex2 = ("DROP INDEX IF EXISTS idxNeedsPart;") 
    dropIndex3 = ("DROP INDEX IF EXISTS Optimizeidx;")

    for name, cardinality in DATABASE_INFO:
        connection = sqlite3.connect(name)
        cursor = connection.cursor()

        cursor.execute(dropIndex1) #drop both indexes to make sure neither alter results
        cursor.execute(dropIndex2)
        cursor.execute(dropIndex3)

########### Start of query ############

#start timer
        startTime = time.perf_counter()
#run 100 times with random country code
        for numExecutions in range(0,100):
            cursor.execute(Query1)
        
        connection.commit()
#end timer. then calculate difference betweeen the times.
        stopTime = time.perf_counter()
        avgTime = ((stopTime - startTime)*1000)
        print("Average time to run query 100 times in database {}: {} milliseconds ({} milliseconds per query)".format(name,avgTime, avgTime/100))
        ########### end of first query ##########

########### Start of query2 ############

#start timer
        startTime = time.perf_counter()
#run 100 times with random country code
        for numExecutions in range(0,100):
            cursor.execute(Query2)
        
        connection.commit()
#end timer. then calculate difference betweeen the times.
        stopTime = time.perf_counter()
        avgTime = ((stopTime - startTime)*1000)
        print("Average time to run query 100 times in database {}: {} milliseconds ({} milliseconds per query)".format(name,avgTime, avgTime/100))
        ########### end of second query ##############

#close database connection.
        connection.close()





def runOptimizeQuery4(): 
    global connection, cursor

    dropIndex1 = ("DROP INDEX IF EXISTS idxMadeIN;")
    dropIndex2 = ("DROP INDEX IF EXISTS idxNeedsPart;") 
    dropIndex3 = ("DROP INDEX IF EXISTS Optimizeidx;")

    createIndex4 = ("CREATE INDEX IF NOT EXISTS Optimizeidx4 ON Parts(needsPart DESC);")

    Query2 = """SELECT COUNT(partNumber) FROM Parts WHERE partNumber NOT IN (needsPart)"""

    for name, cardinality in DATABASE_INFO:
        connection = sqlite3.connect(name)
        cursor = connection.cursor()

        cursor.execute(dropIndex1) #drop both indexes to make sure neither alter results
        cursor.execute(dropIndex2)
        cursor.execute(dropIndex3)

        cursor.execute(createIndex4)


########### Start of query 6 ############

#start timer
        startTime = time.perf_counter()
        #run 100 times with random country code
        for numExecutions in range(0,100):
            cursor.execute(Query2)
        
        connection.commit()
        #end timer. then calculate difference betweeen the times.
        stopTime = time.perf_counter()
        avgTime = ((stopTime - startTime)*1000)
        print("Average time to run query 100 times in database {}: {} milliseconds ({} milliseconds per query)".format(name,avgTime, avgTime/100))
        ########### end of second query ##############

        #close database connection.
        connection.close()





def loadUPC():
    global connection, cursor
    connection = sqlite3.connect("./UPC.db")
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM upc")
    for value in cursor.fetchall():
        upc_code = value[0]
        upc_codes.append(upc_code)
    connection.commit()
    connection.close()

def loadCountries():
    global connection, cursor
    connection = sqlite3.connect("./Country.db")
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM country")
    for value in cursor.fetchall():
        country_code = value[0]
        countries.append(country_code)
    connection.commit()
    connection.close()

def main():
    global connection, cursor
    print("executing part 3, task 13 and 14")
    loadCountries()
    loadUPC()
    runQuery()
    print("executing part 4, task 17")
    runOptimizeQuery4()

main()