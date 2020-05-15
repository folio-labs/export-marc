import pymarc
from pymarc import MARCReader, MARCWriter, Field
import psycopg2
import psycopg2.extras
import time

#WRITTEN W/PYTHON 3.7.3


print("...starting export");

# constructing file name
timestr = time.strftime("%Y%m%d-%H%M%S")
save_file = 'c:\\YOUR\\FILE\\PATH\\' + timestr + ".mrc"
#save_file = '/home/mis306/' + timestr + ".mrc"

#LOCAL DB
DATABASE_HOST = "localhost"
DATABASE_USERNAME = "USERNAME"
DATABASE_PASSWORD = "PASSWORD"
DATABASE_PORT = 1234
DATABASE_NAME = "folio"

def lookupReferenceValues(refTables):
	folio_db = psycopg2.connect(user = DATABASE_USERNAME, password = DATABASE_PASSWORD, 
								host = DATABASE_HOST, port = DATABASE_PORT, database = DATABASE_NAME)
	cursor = folio_db.cursor(cursor_factory=psycopg2.extras.DictCursor)
	lookupTable = {}
	for i in refTables:
		print(i)
		cursor.execute("select jsonb->>'id' as id, jsonb->>'name' as name from " + i)
		rows = cursor.fetchall()
		for row in rows:
			lookupTable[row['id']] = row['name']


	cursor.close()
	folio_db.close()
	return lookupTable

#-->INITIALIZE REFERENCE VALUES (UUIDS TRANSLATE TO NAMES)
tenant = "lu" #CHANGE TO INPUT VALUE?
refTables = [tenant + "_mod_inventory_storage.material_type",
			 tenant + "_mod_inventory_storage.classification_type",
			 tenant + "_mod_inventory_storage.call_number_type",
			 tenant + "_mod_inventory_storage.location"]
refValuesLookup = lookupReferenceValues(refTables)


writer = MARCWriter(open(save_file,'ab'))

try:
	folio_db = psycopg2.connect(user = DATABASE_USERNAME, password = DATABASE_PASSWORD, 
								host = DATABASE_HOST, port = DATABASE_PORT, database = DATABASE_NAME)
	cursor = folio_db.cursor(cursor_factory=psycopg2.extras.DictCursor)
	#-->GET THE IDs FOR EACH NON-DISCOVERY-SUPPRESSED INSTANCE
	cursor.execute("select id from " + tenant +"_mod_inventory_storage.instance where jsonb->>'discoverySuppress'='false' limit 100")
	
	rows = cursor.fetchall()
	marcRecordCursor = folio_db.cursor(cursor_factory=psycopg2.extras.DictCursor)
	for row in rows:
		try: 
			instanceId = row['id'];
			#--> GET THE RAW RECORD ID
			cursor.execute("select jsonb->>'rawRecordId' as rawrecordid from " + tenant + "_mod_source_record_storage.records " \
				"where jsonb->'externalIdsHolder'->>'instanceId' = '" + instanceId + "'")
			recordRow = cursor.fetchone()
			rawRecordId = recordRow['rawrecordid']
			#-->GET THE RAW RECORD
			marcRecordCursor.execute("select id, jsonb->>'content' as marc from " + tenant + "_mod_source_record_storage.raw_records "\
				"where id = '" + rawRecordId + "'")
			marcRow = marcRecordCursor.fetchone()
			marcAsString = marcRow['marc']
			

			for record in MARCReader(marcAsString.encode('iso8859-1')):
				#ADD A 998 FOR EACH HOLDING
				cursor.execute("select id, creation_date, callnumbertypeid, jsonb->>'permanentLocationId' as permanentlocationid, jsonb->>'callNumber' " \
					"as callnumber from " + tenant + "_mod_inventory_storage.holdings_record where instanceid = '" + instanceId + "' and jsonb->>'discoverySuppress'='false'") 
				holdingsRows = cursor.fetchall()
				for holdings in holdingsRows:
					print("in item holdings")
					creationDate = str(holdings['creation_date'])
					holdingsId = holdings['id']
					callNumberType = refValuesLookup[holdings['callnumbertypeid']]
					holdingLocation = refValuesLookup[holdings['permanentlocationid']]
					callNumber = holdings['callnumber']
					print (callNumber + " " + holdingLocation + " " + callNumberType)
					record.add_field(
					Field(
					tag = '998',
					indicators = [' ',' '],
					subfields = [
						'a',callNumber,
						'd',callNumberType,
						'u',creationDate,
						'i',holdingLocation]))
					#ADD A 097 FOR EACH ITEM
					cursor.execute("select id, materialtypeid, jsonb->>'permanentLocationId' as permanentlocationid, "\
						"jsonb->>'barcode' as barcode from " + tenant + "_mod_inventory_storage.item where holdingsrecordid = '" + holdingsId + "' and jsonb->>'discoverySuppress'='false'")
					itemRows = cursor.fetchall()
					for item in itemRows:
						barcode = item['barcode']
						itemLocation = refValuesLookup[item['permanentlocationid']]
						itemType = refValuesLookup[item['materialtypeid']]
						print (barcode + " " + itemLocation + " " + itemType)
						record.add_field(
						Field(
						tag = '097',
						indicators = [' ',' '],
						subfields = [
							'i',barcode,
							'k',itemLocation,
							't',itemType]))
				# write MARC to output file
				writer.write(record)
		except Exception as e:
			print("ERROR PROCESSING ROW:" + str(row))
			print(e)
			print("INSTANCE ID: " + instanceId)
			continue
			

except (Exception, psycopg2.Error) as error :
	print ("Error: ", error)
finally:
    #closing database connection.
	if(folio_db):
		cursor.close()
		marcRecordCursor.close()
		folio_db.close()
		print("PostgreSQL connection closed")
	print("...export complete");
	print("MARC FILE CREATED: " + save_file)
	writer.close()