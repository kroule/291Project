##PART (Application) 2
#SQL STATEMENT 1: SELECT AVG(partPrice) FROM Parts GROUP BY madeIn


import sqlite3
import time

connection = None
cursor = None

DATABASE_INFO = [('./A4v100.db', 100), ('./A4v1k.db', 1000),('./A4v10k.db', 10000), ('./A4v100k.db', 100000), ('./A4v1M.db',1000000)]

#upc_codes = []
#countries = []

def runQuery():
	global connection, cursor
	Query = """SELECT AVG(partPrice) FROM Parts GROUP BY madeIn"""
	dropIndex1 = ("DROP INDEX IF EXISTS idxNeedsParts;")#Drop index if exists, if this does not happen, could alter results
	dropIndex2 = ("DROP INDEX IF EXISTS idxMadeIN;")
	dropIndex3 = ("DROP INDEX IF EXISTS Optimizeidx3;")
	dropIndex4 = ("DROP INDEX IF EXISTS Optimizeidx4;")

	for name, cardinality in DATABASE_INFO:
		connection = sqlite3.connect(name)
		cursor = connection.cursor()

		cursor.execute(dropIndex1)#dropping indexs
		cursor.execute(dropIndex2)
		cursor.execute(dropIndex3)
		cursor.execute(dropIndex4)


		startTimer = time.perf_counter()
		for queryExecution in range(0, 100):
			cursor.execute(Query)

		connection.commit()
		stopTimer = time.perf_counter()
		avgQueryTime = ((stopTimer - startTimer)*1000)
		print("Average time to run query 100 times in datatbase {}: {} milliseconds ({} milliseconds per query)".format(name, avgQueryTime, avgQueryTime/100))

		connection.close()
#run query with index

def runQueryIndex():
	global connection, cursor
	Query = """SELECT AVG(partPrice) FROM Parts GROUP BY madeIn"""
	dropIndex1 = ("DROP INDEX IF EXISTS idxNeedsParts;")#Drop index if exists, if this does not happen, could alter results
	dropIndex2 = ("DROP INDEX IF EXISTS idxMadeIn;")
	dropIndex3 = ("DROP INDEX IF EXISTS Optimizeidx3;")
	dropIndex4 = ("DROP INDEX IF EXISTS Optimizeidx4;")
	createIndex = ("CREATE INDEX idxMadeIn ON Parts ( MadeIn );") #create index that we want to speed up query time.

	for name, cardinality in DATABASE_INFO:

		connection = sqlite3.connect(name)
		cursor = connection.cursor()
		
		cursor.execute(dropIndex1)#dropping indexs
		cursor.execute(dropIndex2)
		cursor.execute(dropIndex3)
		cursor.execute(dropIndex4)
		cursor.execute(createIndex)#creating index

		startTimer = time.perf_counter()
		for queryExecution in range(0, 100):
			cursor.execute(Query)

		connection.commit()
		stopTimer = time.perf_counter()
		avgQueryTime = ((stopTimer - startTimer)*1000)
		print("Average time to run query 100 times in datatbase {}: {} milliseconds ({} milliseconds per query)".format(name, avgQueryTime, avgQueryTime/100))

		connection.close()
def main():
	global connection, cursor
	print("Executing part 2, task 1")
	runQuery()
	print("Executing part 2, task 2")
	runQueryIndex()
main()
