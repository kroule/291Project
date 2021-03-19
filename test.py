def main():

    upc_data = []
    upcfile = open("upc_corpus.csv", "r", encoding="utf8")
    raw_data = upcfile.read().splitlines()
    for line in raw_data:
        upc = line.split(',')[0]
        val = line.split(',')[1]
        
    upc_size = len(raw_data)
    
    random_100 = []
    random_1000 = []
    random_10000 = []
    random_100000 = []
    random_1000000 = []
    
    
        
    upcfile.close() 
    
main()