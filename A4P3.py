#PART (Application) 3
#Drop the index idxMadein, again for all databases, created above.
#Write a Python application (A4P3.py) that embeds Sql statements for executing the following query as per the requirements in the task below.
#   Q4: Find the most expensive part made in a randomly selected country (code) that exists in parts
#Tasks:
#  8 Run Q4 100 times (each time using a randomly generated value for the country code) and record the average query time required for the database to answer Q4 queries using each version of table Parts.
#  10 Define and create one INDEX with the goal of optimizing query Q4 (take note of its space cost).
#  11 Repeat Task 9, i.e., Run Q4 100 times and record the average query time required for the database to answer Q4 queries using each version of table Parts.
#  12 Compare and contrast the results obtained in Tasks 9 and 10. Discuss in which cases the space cost of the index seemed like a worthwhile "investment.”

#  SQL STATEMENT 1: SELECT MAX(partPrice) FROM Parts WHERE madeIn=?
#  SQL Index optimization: CREATE INDEX IF NOT EXISTS Optimizeidx ON Parts(partPrice DESC); 


import sqlite3
import time
import random

connection = None
cursor = None

##### create the databases with appropriate cardinality ######
DATABASE_INFO = [('./A4v100.db', 100), 
                ('./A4v1k.db',1000),
                ('./A4v10k.db', 10000),
                ('./A4v100k.db', 100000),
                ('./A4v1M.db', 1000000)]
countries = []
upc_codes = []


################################################### START OF QUERY ######################################################

def runQuery(): 
    global connection, cursor

    dropIndex1 = ("DROP INDEX IF EXISTS idxMadeIN;")
    dropIndex2 = ("DROP INDEX IF EXISTS idxNeedsPart;") 
    dropIndex3 = ("DROP INDEX IF EXISTS Optimizeidx3;")
    dropIndex4 = ("DROP INDEX IF EXISTS Optimizeidx4;")

    #  SQL STATEMENT 1: SELECT MAX(partPrice) FROM Parts WHERE madeIn=?
    Query = """SELECT MAX(partPrice) FROM Parts WHERE madeIn=?"""

    for name, cardinality in DATABASE_INFO:
        connection = sqlite3.connect(name)
        cursor = connection.cursor()

        cursor.execute(dropIndex1) #drop all the indexes to make sure neither alter results
        cursor.execute(dropIndex2)
        cursor.execute(dropIndex3)
        cursor.execute(dropIndex4)

########### Start of query ############

#start timer
        startTime = time.perf_counter()
#run 100 times with random country code
        for numExecutions in range(0,100):
            randCountrycode = random.choice(countries)  #execute the query with the random value
            cursor.execute(Query, (randCountrycode,))
        
        connection.commit()
#end timer. then calculate difference betweeen the times.
        stopTime = time.perf_counter()
        avgTime = ((stopTime - startTime)*1000)
        print("Average time to run query 100 times in database {}: {} milliseconds ({} milliseconds per query)".format(name,avgTime, avgTime/100))
#close database connection.
        connection.close()

################################################### END OF QUERY ######################################################



################################################### START OF OPTIMIZATION QUERY ######################################################

def runOptimizeindex():
    global connection, cursor
    

    dropIndex1 = ("DROP INDEX IF EXISTS idxMadeIN;")
    dropIndex2 = ("DROP INDEX IF EXISTS idxNeedsPart;")
    dropIndex3 = ("DROP INDEX IF EXISTS Optimizeidx3;")
    dropIndex4 = ("DROP INDEX IF EXISTS Optimizeidx4;")

    #  SQL Index optimization: CREATE INDEX IF NOT EXISTS Optimizeidx ON Parts(partPrice DESC); 
    createIndex3 = ("CREATE INDEX IF NOT EXISTS Optimizeidx ON Parts(partPrice DESC);")

    #  SQL STATEMENT 1: SELECT MAX(partPrice) FROM Parts WHERE madeIn=?
    Query = """SELECT MAX(partPrice) FROM Parts WHERE madeIn=?"""

    for name, cardinality in DATABASE_INFO:
        connection = sqlite3.connect(name)
        cursor = connection.cursor()

        cursor.execute(dropIndex1)  #drop all possible made index's
        cursor.execute(dropIndex2)
        cursor.execute(dropIndex3)
        cursor.execute(dropIndex4)

        cursor.execute(createIndex3) #new index
        startTime= time.perf_counter()

        for numrunIndex in range(0,100):
            randCountrycode = random.choice(countries)
            cursor.execute(Query, (randCountrycode,)) #execute the query with the random value
        connection.commit()
        endTime = time.perf_counter()
        avgTime = ((endTime - startTime)*1000)
        print("Average time to run query optimized 100 times in database {}: {} milliseconds ({} milliseconds per query)".format(name,avgTime, avgTime/100))

        connection.close()

########################################################### END OF OPTIMIZATION QUERY ################################################


## create the index's for UPC codes and country codes ##
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


#main function will execute the query and the optimized query# 
def main():
    global connection, cursor
    print("executing part 3, task 9")
    loadCountries()
    loadUPC()
    runQuery()
    print("Executing Part 3, task 11")
    runOptimizeindex()

main()