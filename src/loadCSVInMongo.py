from datetime import datetime
import pymongo
from pymongo import Connection
import sys
import os
import string
import re

def storeInMongo(smsdata, obj, fieldTypeMap):
    try:
	for k in obj:
	    if fieldTypeMap.has_key(k) and fieldTypeMap[k] == 'datetime':
		ts = obj[k]
		if ts.strip() == '': continue
		try:
		    obj[k]= datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
		except:
		    pass
	    if fieldTypeMap.has_key(k) and fieldTypeMap[k] == 'int':
	        try:
		    obj[k] = int(obj[k])
		except:
		    num = obj[k]
		    obj[k] = re.sub("[^0-9]*", "", num)
	print "insert object"
	objectId = smsdata.insert(obj)
    except:
	print "Unexpected error:", sys.exc_info()

def readFile():
   filename = sys.argv[1]
   keys = []
   fieldTypes = []
   if len(sys.argv) < 2:
       print "correct command datafile.csv fieldsTxtFile fieldTypeTxtFile"   
   if len(sys.argv) > 2:
       fieldFile = sys.argv[2]
       if not os.path.exists(fieldFile):
           print "Field file does not exists: %s"%fieldFile
	   exit(0)
       keysTxt = open(fieldFile).read()
       keys = map(lambda x: x.strip(), keysTxt.split(","))
       print "keys ", len(keys), keys 
   if len(keys) == 0:
       print "No Field Names"
       exit(0)
   fieldTypeMap = {}
   if len(sys.argv) > 3:
       fieldTypeFile = sys.argv[3]
       if not os.path.exists(fieldTypeFile):
           print "Field Type File does not exists: %s"%fieldTypeFile
           exit (0)
       fieldTypeTxt = open(fieldTypeFile).read()
       fieldTypes = map(lambda x: x.strip(), fieldTypeTxt.split(","))
       fieldTypeTupleList = map(None, keys, fieldTypes)
       fieldTypeList = map(lambda x:{x[0]:x[1]}, fieldTypeTupleList)
       fieldTypeMap.update(fieldTypeList[0]) 
       reduce(lambda x,y: fieldTypeMap.update(y), fieldTypeList)       
       print fieldTypeMap
 
   if not os.path.exists(filename):
       print "File doesnt exist %s"%filename
       sys.exit(0)    	
   fp = open(filename)
   connection = Connection("localhost", 27017)
   db = connection.mydb
   smsdata = db.smsmagic
   for line in fp:
       try:
           rawValues = line.split(",")
	   if len(rawValues) not in [len(keys), len(keys) -1]: 
		#print "less number of columns, skipping ", len(rawValues), len(keys)
		continue
	   values =  map(lambda a: a.strip('"'), rawValues)
	   listOfTuples = map(None,keys, values)
           jsonObjList = map(lambda x:{x[0]:x[1]}, listOfTuples)
	   jsonObj = {}
	   jsonObj.update(jsonObjList[0])
	   if len(rawValues) == len(keys)-1:
      	       jsonObj["formattedMobileNumber"] = "0"
	   reduce(lambda x,y: jsonObj.update(y), jsonObjList)
	   storeInMongo(smsdata, jsonObj, fieldTypeMap)
       except:
           pass
           #print "wrong input %s"%line 
       	 
def main():
   readFile()

if __name__ == '__main__':
   main()
