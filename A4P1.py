#PART (Application) 1 
#Write a Python application (A4P1.py) that embeds SQL statements for executing the following two queries as per the requirements in the tasks below:
#	Q1: Given a randomly selected UPC code U from the UPC database find the price of part in Parts that has partNumber = U
#	Q2: Given a randomly selected UPC code U from the UPC database find the price of part in Parts that has needsPart = U
#Tasks:
#	Run each query 100 times (each time using a randomly generated value for U) against each version of table Parts and record the average query time required for answering both queries using each version of table Parts.
#	Compare the query times (for queries Q1 and Q2) and, contrast the results obtained, i.e., explain why they are similar or different. 
#Now create the following index, for all 5 different versions of the database:
#	CREATE INDEX idxNeedsPart ON Parts ( needsPart );
#	and take note of the approximate size of index (if would suffice to compare the sizes of the .db file before and after creation of the index). This will help you evaluate the space cost (and benefit) of the index. 
#Tasks:
#	Re-run queries Q1 and Q2 100 times against each version of table Parts and record the average query time required for the dbms to answer both queries using each version of table Parts.
#	Compare the two tables (for queries Q1 and Q2) in Task 3 contrasting the results obtained. 
#	Compare the trends observed in Task 4 to the trends observed in Task 2. Discuss in which cases the space cost of the index seemed like a worthwhile "investment"

import sqlite3
import time
import random
connection = None
cursor = None
DATABASE_INFO = [('./A4v100.db', 100), 
                       ('./A4v1k.db', 1000), 
                       ('./A4v10k.db', 10000), 
                       ('./A4v100k.db', 100000),
                       ('./A4v1M.db',1000000)]

countries = []
upc_codes = []

#The below two functions do the same thing (differ in performance for some reason), but the hardcoded is easier to read

def runQueriesHardCoded():
	global connection, cursor
	Q1 = """SELECT partPrice FROM Parts WHERE partNumber=?"""
	
	Q2 = """SELECT partPrice FROM Parts WHERE needsPart=?"""
	for name, cardinality in DATABASE_INFO:
        # Connect to each database in the table
		connection = sqlite3.connect(name)
		cursor = connection.cursor()
        
        ############################# Start of first query #############################
        
        #start timer
		time_start = time.perf_counter()
        
        # Run first query 100 times	
		for numExecutions in range(0,100):
			randPartNumber = random.choice(upc_codes)  
			cursor.execute(Q1, (randPartNumber,))
            
		connection.commit()
        
        #end timer and calculate difference between times
		time_end = time.perf_counter()
		avg_time = ((time_end-time_start)*1000)
		print("Average time to run Q1 100 times in database {}: {} milliseconds ({} millieseconds per query)".format(name, avg_time, avg_time/100))
        ############################# End of first query #############################
		 
        ############################# Start of second query #############################
        
        #start timer
		time_start = time.perf_counter()
        
        # Run second query 100 times
		for numExecutions in range(0,100):
			randPartNumber = random.choice(upc_codes)	
			cursor.execute(Q2, (randPartNumber,))
            
		connection.commit()
        
        #ebd tuner abd calculate difference between times
		time_end = time.perf_counter()
		avg_time = ((time_end-time_start)*1000)
		print("Average time to run Q2 100 times in database {}: {} milliseconds ({} millieseconds per query)".format(name, avg_time, avg_time/100))		
		connection.commit()
        ############################# End of second query #############################
	
        # Close db connection, loop back to top and open next database
		connection.close()
    
def runQueryIndex():
	global connection, cursor
	createIndex = ("CREATE INDEX IF NOT EXISTS idxNeedsPart ON Parts(needsPart);")

	Query1 = """SELECT partPrice FROM Parts WHERE partNumber=?"""
	Query2 = """SELECT partPrice FROM Parts WHERE needsPart=?"""
	
	for name, cardinality in DATABASE_INFO:	
		connection = sqlite3.connect(name)
		cursor = connection.cursor()
		cursor.execute(createIndex)
		startTimeIndex = time.perf_counter()
		
		for numRunsIndex in range(0, 100):
			randPartNumber = random.choice(upc_codes)
			cursor.execute(Query1, (randPartNumber,))
		connection.commit()
		endTimeIndex = time.perf_counter()
		avgTimeIndex = ((endTimeIndex-startTimeIndex)*1000)
		print("Average time to run Query 1 100 times in database {}: {} millieseconds ({}milliseconds per query)".format(name, avgTimeIndex, avgTimeIndex/100))

		startTimeIndex = time.perf_counter()
		for numRunsIndex in range(0, 100):
			randPartNumber = random.choice(upc_codes)
			cursor.execute(Query2, (randPartNumber,))
		connection.commit()
		endTimeIndex = time.perf_counter()
		avgTimeIndex = ((endTimeIndex-startTimeIndex)*1000)
		print("Average time to run Query 2 100 times in database {}: {} millieseconds ({}milliseconds per query)".format(name, avgTimeIndex, avgTimeIndex/100))
		connection.commit()
		connection.close()
"""
def runQueries():
	global connection, cursor
    #Q1: Given a randomly selected UPC code U from the UPC database find the price of part in Parts that has partNumber = U
    #Q2: Given a randomly selected UPC code U from the UPC database find the price of part in Parts that has needsPart = U
	queries = ["SELECT partPrice FROM Parts WHERE partNumber=?", "SELECT partPrice FROM Parts WHERE needsPart=?"]
    
    #Run each query 100 times (each time using a randomly generated value for U) against each version of table Parts and record the average query time required for answering both queries using each version of table Parts.
	for name, cardinality in DATABASE_INFO:
		connection = sqlite3.connect(name)
		cursor = connection.cursor()
        
		for query in queries:
			time_start = time.perf_counter()
			for numExecutions in range(0,100):
				randPartNumber = random.choice(upc_codes)  
				cursor.execute(query, (randPartNumber,))
				val = cursor.fetchone()
			connection.commit()
			time_end = time.perf_counter()
			avg_time = ((time_end-time_start)*1000)
			print("Average time to run {} 100 times in database {}: {} milliseconds ({} millieseconds per query)".format(query, name, avg_time, avg_time/100))
		connection.commit()
		connection.close()
"""
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
	print("Executing Part 1, task 1")
	loadCountries()
	loadUPC()
	runQueriesHardCoded()
	print("Executing Part1, task 2")
	runQueryIndex()

main()
