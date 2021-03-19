import sqlite3
import random
import time

create_parts_query = """CREATE TABLE Parts (
                        partNumber UNSIGNED BIG INT,
                        partPrice INTEGER,
                        needsPart UNSIGNED BIG INT,
                        madeIn CHAR(2),
                        PRIMARY KEY(partNumber)
                        );"""

#the following are just enumerated tables for easy use of 'randon.randint(0,table_size)

create_upc_query = """CREATE TABLE upc (partNumber UNSIGNED BIG INT,
                                        PRIMARY KEY(partNumber));"""

create_country_query = """CREATE TABLE country (country CHAR(2),
                                                PRIMARY KEY(country));"""


#    You will then create five different databases, each containing a different version of the table. Each version will differ in its cardinality NP, and we'll use five values for NP, namely 100, 1,000, 10,000, 100,000 and 1,000,000, in order to check how SQLite's performance will scale. These databases must be called, A4v100.db, A4v1k.db, A4v10k.db, A4v100k.db and A4v1M.db, respectively. This is because we'll use our own databases during marking and also to avoid you having to submit them -- we don't have to store almost 1,000 .db files on eClass. 

DATABASES_TO_CREATE = [('./A4v100.db', 100), 
                       ('./A4v1k.db', 1000), 
                       ('./A4v10k.db', 10000), 
                       ('./A4v100k.db', 100000),
                       ('./A4v1M.db',1000000)]

def create_databases(upc_data, country_data):
    
    # We will use the bottom half of the list for simplicity sake here
    # We assign the bottom 1 million values of the list, a random 'needsPart' which is not itself
    # If we do happen to randomly select it's own value then just try again.
    rnd_needs_part = []
    for i in range(0,1000000):
        needs_part = ''
        while(needs_part == '' or needs_part==upc_data[i]):
            needs_part = random.choice(upc_data)
        rnd_needs_part.append(needs_part)
            
    for dbpath, cardinality in DATABASES_TO_CREATE:
        connection = sqlite3.connect(dbpath)
        cursor = connection.cursor()
        cursor.execute('PRAGMA forteign_keys=ON;')
        cursor.execute(create_parts_query)
        connection.commit()        
        
        upc_data_size = len(upc_data)
        for i in range(0, cardinality):
            upc, price, requpc, country = int(upc_data[i]), random.randint(1,100), int(rnd_needs_part[i]), random.choice(country_data)
            try:
                cursor.execute("INSERT INTO Parts VALUES(?,?,?,?)", (upc, price, requpc, country))
            except sqlite3.Error as e:
                print("{}, {}, {}, {}".format(upc, price, requpc, country))
                print(e)
                connection.close()
                    
        connection.commit()    
        connection.close()

#Taken from https://stackoverflow.com/questions/1265665/how-can-i-check-if-a-string-represents-an-int-without-using-try-except
# will cite properly later
def validInt(strVal):
    try: 
        int(strVal)
        return True
    except ValueError:
        return False        

def main():

    upc_data = []
    print("open upc file start: ", time.perf_counter())
    upcfile = open("upc_corpus.csv", "r", encoding="utf8")
    raw_data = upcfile.read().splitlines()
    upcfile.close()
    
    # used to skip first item in list (headers) - better way to do this if we have time
    skipped_first = False
    for line in raw_data:
        if(skipped_first == False):
            skipped_first=True
            continue
        
        # We don't actually need the 'UPC Name' - just the number
        upc_code = line.split(',')[0]
        if(upc_code=='null' or upc_code==''):
            continue
        if(validInt(upc_code) == False):
            continue

        #UPC codes only valid if they are less than or equal to 12 characters in length
        #The list actually uses EAN codes which are 13 characters in length.
        #The size of the list shrinks below 1 million if we remove anything less than 13
        if(len(upc_code) > 13):
            continue
        upc_data.append(upc_code)
        
        
    print("Removing invalid UPC start: ", time.perf_counter())

    print("Removing invalid UPC start: end", time.perf_counter())
        
    print("process upc file end: ", time.perf_counter())
    # There are 2649 'null' or '' UPC columns (2492 NULL, 157 '')
    upc_size = len(upc_data)
    # There are 7403 duplicate UPC codes
    print("removing duplicates from list...", time.perf_counter())
    # https://www.w3schools.com/python/python_howto_remove_duplicates.asp stolen from here
    upc_data = list( dict.fromkeys(upc_data) )
    print("duplicates removed...", time.perf_counter())
    
    print("creating enumerated UPC.db start", time.perf_counter())
    
    connection = sqlite3.connect("./UPC.db")
    cursor = connection.cursor()
    cursor.execute(create_upc_query)
    for upc in upc_data:
        cursor.execute("INSERT INTO upc VALUES(?)", (upc,))
    connection.commit()
    connection.close()
    
    print("created enumerated UPC.db end", time.perf_counter())
    
    print("shuffle data start: ", time.perf_counter())
    random.shuffle(upc_data)
    print("shuffled data end: ", time.perf_counter())
    country_data = []
    print("open countries file start: ", time.perf_counter())
    upcfile = open("countries.csv", "r", encoding="utf8")
    raw_data = upcfile.read().splitlines()
    upcfile.close()
    
    skipped_first = False
    
    for line in raw_data:
        if(skipped_first == False):
            skipped_first=True
            continue
            
        split_line = line.split(",")
        
        #Probably don't even need the below since we only need the last 'column' even with incorrect split(',') stuff
        #This handles the case where there is more than one comma within one of the country names
        # "Virgin Islands, U.S", "VI" becomes ("Virgin Islands", "U.S", "VI")
        # then we take the first_entry = [0], the we continue to append items in the last up until the last entry [len-1]
        # then set the tuple to first_entry, last_entry - It's ugly but it works for this specific case
        if len(split_line) > 2:
            first_entry = split_line[0]
            for i in range(1, len(split_line)-1):
                first_entry = first_entry + split_line[i]
            split_line = [first_entry, split_line[len(split_line)-1]]
            split_line[0] = split_line[0].strip('"')
            
        if(split_line[0]=='null' or split_line[0]==''):
            continue
        
        #Only need the country abbreviation
        country_data.append(split_line[1])    
        
    print("process country file end: ", time.perf_counter())
    
    print("created enumerated Country.db start", time.perf_counter())
    connection = sqlite3.connect("./Country.db")
    cursor = connection.cursor()
    cursor.execute(create_country_query)
    for country in country_data:
        cursor.execute("INSERT INTO country VALUES(?)", (country,))

    connection.commit()
    connection.close()    
    print("created enumerated Country.db end", time.perf_counter())    
    
    print("creating databases start: ", time.perf_counter())
    create_databases(upc_data, country_data)
    print("creating databases end: ", time.perf_counter())
     
main()


