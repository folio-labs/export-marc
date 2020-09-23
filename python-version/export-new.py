import pymarc
from pymarc import JSONReader, Field, JSONWriter, XMLWriter
import psycopg2
import psycopg2.extras
import time
import logging

#WRITTEN W/PYTHON 3.7.3


print("...starting export");

# constructing file and log name
timestr = time.strftime("%Y%m%d-%H%M%S")
logging.basicConfig(filename=timestr + "-export.log")

#LOCAL DB
DATABASE_HOST = "redacted"
#DATABASE_HOST = "localhost"
DATABASE_USERNAME = "redacted"
DATABASE_PASSWORD = "redacted"
DATABASE_PORT = 5432
DATABASE_NAME = "folio"
TENANT = "lu"

count = 0
folio_db = psycopg2.connect(
	user=DATABASE_USERNAME,
	password=DATABASE_PASSWORD,
	host=DATABASE_HOST,
	port=DATABASE_PORT,
	database=DATABASE_NAME
)
cursor = folio_db.cursor(name='folio',cursor_factory=psycopg2.extras.DictCursor)
#THIS COULD BE MODIFIED TO RETREIVE X NUMBER OF RECORDS PER FILE
cursor.itersize=300000
#from {}_mod_marc_storage.marc_record'''.format(TENANT)
select_ids_sql = '''
select
id, 
instance_id 
from {}_mod_source_record_storage.records_lb where state = {} and suppress_discovery = False'''.format(TENANT,"'ACTUAL'")
print("executing query")
cursor.execute(select_ids_sql)
while True:
	print("in the while true - fetching...")
	rows = cursor.fetchmany(cursor.itersize)
	print("fetch is done")
	marcRecordCursor = folio_db.cursor(cursor_factory=psycopg2.extras.DictCursor)
	if rows:
		save_file = timestr + "." + str(count) + ".json"
		writer = open(save_file,'wt')
		print("created the file: " + save_file)
		count += 1
		for row in rows:
			try: 
				rowId = row['id'];
				rowInstanceId = row['instance_id'];
				if rowInstanceId == None:
						logging.error("BAD RECORD: INSTANCE ID WAS NULL" + str(row))
						continue
				select_record_sql = '''
				select id, 
				content as marc
				from {}_mod_source_record_storage.marc_records_lb where 
				id = '{}' limit 1'''.format(TENANT, rowId)
				#print(select_record_sql)
				marcRecordCursor.execute(select_record_sql)
				marcRow = marcRecordCursor.fetchone()
				marcJsonAsString = marcRow['marc']
				marcString = marcJsonAsString.encode('utf-8').strip()
				#print(marcJsonAsString);
				for record in JSONReader(marcJsonAsString):
					#write MARC JSON to output file
					#ADD A 998 FOR EACH HOLDING RECORD
					if record['6xx'] is not None:
						logging.error("BAD RECORD: 6xx" + str(row))
						continue
					if record['4xx'] is not None:
						logging.error("BAD RECORD: 4xx" + str(row))
						continue
					select_holding_sql = '''
					select id, creation_date, callnumbertypeid, 
					jsonb->>'permanentLocationId' as permanentLocationId, 
					jsonb->>'callNumber' as callNumber from 
					{}_mod_inventory_storage.holdings_record 
					where instanceid = '{}'  and jsonb->>'discoverySuppress'='false' '''.format(TENANT,rowInstanceId)
					#print(select_holding_sql)
					marcRecordCursor.execute(select_holding_sql)
					holdingRows = marcRecordCursor.fetchmany()
					for holding in holdingRows:
						#print(holding['callnumber'])
						rowHoldingsId = holding['id']
						record.add_field(
							Field(tag = '998',
								  indicators = [' ',' '],
								  subfields = ['a',holding['callnumber'],
											'l',holding['permanentlocationid']]))
						#ADD AN 097 FOR EACH ITEM
						select_item_sql = '''
						select id, materialtypeid, jsonb->>'permanentLocationId' as 
						permanentLocationId, jsonb->>'barcode' as barcode, jsonb->>'callnumber' as callnumber from 
						{}_mod_inventory_storage.item where 
						holdingsrecordid = '{}' and  jsonb->>'discoverySuppress'='false' '''.format(TENANT,rowHoldingsId)
						#print(select_item_sql)
						marcRecordCursor.execute(select_item_sql)
						itemRows = marcRecordCursor.fetchmany()
						for item in itemRows:
							#print(item['barcode']);
							record.add_field(
								Field(tag = '097',
									indicators = [' ',' '],
									subfields = ['i',item['barcode'],
									'k',item['permanentlocationid'],
									't',item['materialtypeid'],
									'a',item['callnumber']]))
							if (len(record.leader) < 24):
								logging.error("BAD LEADER" + record.leader + " " + str(row))
								record.leader = "{:<24}".format(record.leader)
					writer.write(record.as_json())
					writer.write('\n')
			except Exception as e:
					print("ERROR PROCESSING ROW:" + str(row))
					print(e)
					if rowInstanceId == None:
						rowInstanceId = "None" #FOR LOGGING
					logging.error("UNABLE TO WRITE TO FILE: " + rowInstanceId)
					logging.error(e)
					continue
		writer.close()
	else:
		print("in the else --> finishing")
		break

if (folio_db):
	cursor.close()
	marcRecordCursor.close()
	folio_db.close()
	print("complete")
