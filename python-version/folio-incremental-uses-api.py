import json
import sys
import requests
import psycopg2
import time
import urllib.request, json 
from collections import OrderedDict
from pymarc import JSONReader, Field, JSONWriter, XMLWriter
import time
import logging
from datetime import date, timedelta, datetime


#THIS SCRIPT EXPORTS MARC CONTENT USING THE SOURCE RECORD API
#QUERIES BY LAST UPDATED DATE
#ADDS FIELDS FROM THE HOLDINGS AND ITEMS
#PRODUCES A FILE OF JSON MARC RECORDS THAT VUFIND CAN INDEX

timestr = time.strftime("%Y%m%d-%H%M%S")
logging.basicConfig(filename=timestr+"-export.log")

url = "https://your.okapi.url"
tenant = "your-tenant"


headers = {"x-okapi-tenant": tenant, "Content-type": "application/json"}

#AUTHENTICATE
user = {}
user['username'] = "userid"
user['password'] = "password"
user['tenant'] = tenant
the_data = json.dumps(user)
print(the_data)
response = requests.post(url + "/authn/login",the_data,headers=headers)
print(response)
token = response.headers['x-okapi-token']
print(response)

headers = {"x-okapi-tenant": tenant, "Content-type": "application/json","x-okapi-token":token}

#GET ALL MARC RECORDS THAT WERE UPDATED YESTERDAY? - RUNS EARLY EACH MORNING?
updateDate = (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")
print(updateDate);
#response = requests.get(url + "/item-storage/items?query=(metadata.updatedDate = " + updateDate + ")",headers=headers)
response = requests.get(url + "/source-storage/source-records?updatedAfter=" + updateDate + "&limit=99999",headers=headers)
print(response)
the_data = response.json()

save_file = timestr + ".json"
writer = open(save_file,'wt')
for i in the_data['sourceRecords']:
	#print(i['parsedRecord']["content"])
	marcJsonAsString = i['parsedRecord']['content'];
	instanceId = i['externalIdsHolder']['instanceId']
	m = json.dumps(marcJsonAsString) 
	for record in JSONReader(m):
		#get the item and holding record? 998 and 097
		print (instanceId)
		print(record)
		holdingResponse = requests.get(url + "/holdings-storage/holdings?query=(instanceId=" + instanceId + ")",headers=headers)
		#print(response)
		holdings_data = holdingResponse.json()
		for h in holdings_data['holdingsRecords']:
			holdingsUuid = h.get('id','')
			holdingLocationId = h.get('permanentLocationId','')
			callNumber = h.get('callNumber','')
			record.add_field(
					Field(tag = '998',
							indicators = ['',''],
							subfields = ['a',callNumber,
							             'l',holdingLocationId]))
			itemResponse = requests.get(url + "/item-storage/items?query=(holdingsRecordId==" + holdingsUuid + ")" ,headers=headers)
			item_data = itemResponse.json()
			for i in item_data["items"]:
				barcode = i.get('barcode', '')
				materialType = i.get('materialTypeId','')
				try:
					callNumber = i['effectiveCallNumberComponents']['callNumber']
				except KeyError:
					callNumber = ''
				itemLocation = i.get('permanentLoanTypeId','')
				record.add_field(Field(tag = '097',
										indicators = ['',''],
										subfields = ['i',barcode,
										             'k',itemLocation,
										             't',materialType,
										             'a',callNumber]))

		writer.write(record.as_json())
		writer.write('\n')

writer.close()
