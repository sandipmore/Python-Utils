from datetime import datetime
import pymongo
from pymongo import Connection
import sys
import os
import string
def storeInMongo(smsdata, j):
    try:
	ts = j["createdOn"]
	j["createdOn"]= datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
	objectid = smsdata.insert(j)
    except:
	print "Unexpected error:", sys.exc_info()

def readFile():
   filename = sys.argv[1] 
   if not os.path.exists(filename):
       print "File doesnt exist %s"%filename
       sys.exit(0)    	
    
   fp = open(filename)
   connection = Connection("localhost", 27017)
   db = connection.mydb
   smsdata = db.smsmagic
   keys = ["id","mobilenumber","formattedMobileNumber","senderId","responseId","createdOn","modifiedOn","sentStatus","text","providerResponseId","accountId","deliveryStatus","statusMessage","credits","userId","isInternational","sfExternalfield","serviceProviderId","countryId","euroCredits","encoding","longsmsId"]
   
   for line in fp:
       try:
           rawValues = line.split(",")
	   
	   if len(rawValues) not in [len(keys), len(keys) -1]: 
		print "less number of columns, skipping"
		continue
	   
	   values =  map(lambda a: a.strip('"'), rawValues)
	   listOfTuples = map(None,keys, values)
           jsonObjList = map(lambda x:{x[0]:x[1]}, listOfTuples)
	   jsonObj = {}
	   jsonObj.update(jsonObjList[0])
	   if len(rawValues) == len(keys)-1:
      	       jsonObj["formattedMobileNumber"] = "0"
	   reduce(lambda x,y: jsonObj.update(y), jsonObjList)
	   storeInMongo(smsdata, jsonObj)
       except:
           pass
           #print "wrong input %s"%line 
       	 
def main():
   readFile()

if __name__ == '__main__':
   main()
