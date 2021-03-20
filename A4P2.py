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
	dropIndex = ("DROP INDEX IF EXISTS idxNeedsParts;")

	for name, cardinality in DATABASE_INFO:
		connection = sqlite3.connect(name)
		cursor = connection.cursor()
		cursor.execute(dropIndex)

		startTimer = time.perf_counter()
		for queryExecution in range(0, 100):
			cursor.execute(Query)

		connection.commit()
		stopTimer = time.perf_counter()
		avgQueryTime = ((stopTimer - startTimer)*1000)
		print("Average time to run query 100 times in datatbase {}: {} milliseconds ({} milliseconds per query)".format(name, avgQueryTime, avgQueryTime/100))

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

def loadCOuntries():
	global connection, cursor
	connection = sqlite3.connect("./Country.db")
	cursor = connection.cursor()
	cursor.execute("SELECT * FROM country")
	for value in cursor.fetchall():
		country_code = value[0]
		countries.append(country_code)
	connection.commit()
	connection.close() 
"""
def main():
	global connection, cursor

	runQuery()
main()
