package org.olf.folio.export;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.marc4j.MarcPermissiveStreamReader;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamWriter;
import org.marc4j.MarcWriter;
import org.marc4j.marc.DataField;
import org.marc4j.marc.MarcFactory;
import org.marc4j.marc.Record;
import org.marc4j.marc.Subfield;

public class Export {
	
	private static MarcWriter writer = null;
	public static Logger logger = LogManager.getLogger(Export.class);
	
	static String dbuser = "USERID";
	static String dbpassword = "PASSWORD";
	static String dburl = "localhost";
	static String dbport = "1234";
	static String dbname = "folio";
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException, UnsupportedEncodingException, FileNotFoundException {
		// TODO Auto-generated method stub
		
		System.out.println("...starting export");
		String tenant = "lu"; //CHANGE TENANT TO INPUT ARG!
		
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH.mm.ss");  
		File file = new File("vufind_full_" + df.format(new Date()) +".mrc"); 

		FileOutputStream fileOutputStream = new FileOutputStream(file);
		writer = new MarcStreamWriter(fileOutputStream, true);
		

		Class.forName("org.postgresql.Driver");
		String url = "jdbc:postgresql://" + dburl + ":" + dbport + "/" + dbname + "?user=" + dbuser + "&password=" + dbpassword;
		Connection conn = DriverManager.getConnection(url);
		Statement st = conn.createStatement();

		 //CREATE A LOOKUP TABLE WITH UUIDS/NAMES
		 List<String> referenceTables = new ArrayList<String>(); 
		 referenceTables.add(tenant + "_mod_inventory_storage.material_type");
		 referenceTables.add(tenant + "_mod_inventory_storage.classification_type");
		 referenceTables.add(tenant + "_mod_inventory_storage.call_number_type");
		 referenceTables.add(tenant + "_mod_inventory_storage.location");
		 
		 HashMap<String,String> lookupTable = lookupReferenceValues(referenceTables);
		
		//GET ALL NON "DISCOVERY SUPPRESS" RECORDS
		ResultSet rs = st.executeQuery("select id from lu_mod_inventory_storage.instance where jsonb->>'discoverySuppress'='false' limit 100");

		int counter = 1;
		while (rs.next()) {
			System.out.println(counter);
			counter++;
			String instanceId = rs.getString("id");
			logger.info(instanceId);
			//FIND THE ID FOR THE RAW RECORD
			Statement stTwo = conn.createStatement();
			ResultSet rawRecordIdResult = stTwo.executeQuery("select jsonb->>'rawRecordId' as rawRecordId from lu_mod_source_record_storage.records where jsonb->'externalIdsHolder'->>'instanceId' = '" + instanceId + "'");
			rawRecordIdResult.next();
			String rawRecordId = rawRecordIdResult.getString("rawRecordId");
			//GET THE RAW RECORD
			Statement stThree = conn.createStatement();
			ResultSet rawRecordResult = stThree.executeQuery("select id, jsonb->>'content' as marc from lu_mod_source_record_storage.raw_records where id = '" + rawRecordId+ "'");
			rawRecordResult.next();
			String rawMARC = rawRecordResult.getString("marc");
			Record record = null;
			MarcReader reader = null;
		    logger.info(rawMARC);
		    InputStream in =  new ByteArrayInputStream( rawMARC.getBytes(StandardCharsets.UTF_8) ); 
		    try {
		    	//READ IN AS MARC & WRITE TO MARC FILE
		     	reader= new MarcPermissiveStreamReader(in,true,false);
		    	record = reader.next();
		    	
		    	//ADD A 998 FOR EACH HOLDING
		    	Statement holdingsSt = conn.createStatement();
		    	ResultSet holdingsResultSet = holdingsSt.executeQuery("select id, creation_date, callnumbertypeid, jsonb->>'permanentLocationId' as permanentLocationId, jsonb->>'callNumber' as callNumber from lu_mod_inventory_storage.holdings_record where instanceid = '" + instanceId + "' " );
		    	while (holdingsResultSet.next()) {
		    		 String creationDate = holdingsResultSet.getString("creation_date");
		    		 String callNumber = "no call number";
		    		 callNumber = holdingsResultSet.getString("callNumber");
		    		 String holdingsLocation = lookupTable.get(holdingsResultSet.getString("permanentLocationId"));
		    		 String holdingsId = holdingsResultSet.getString("id");
		    		 String callNumberType = lookupTable.get(holdingsResultSet.getString("callnumbertypeid")); 

		    		 
		    		 DataField field = MarcFactory.newInstance().newDataField();
		    		 field.setTag("998");
		    		 Subfield date = MarcFactory.newInstance().newSubfield('u', creationDate);
		    		 Subfield holdingLocationSubfield = MarcFactory.newInstance().newSubfield('l',holdingsLocation);
				     Subfield callNoType = MarcFactory.newInstance().newSubfield('d',callNumberType);
				     Subfield callNo =   MarcFactory.newInstance().newSubfield('a', callNumber);
		    		 field.addSubfield(date);
		    		 field.addSubfield(holdingLocationSubfield);
		    		 field.addSubfield(callNoType); 
		    		 field.addSubfield(callNo);
		    		 
		    		
		    		 record.addVariableField(field);
		    		 
		    		//ADD AN 097 FOR EACH ITEM
				    Statement itemSt = conn.createStatement();
				    ResultSet itemResultSet = itemSt.executeQuery("select id, materialtypeid, jsonb->>'permanentLocationId' as permanentLocationId, jsonb->>'barcode' as barcode from lu_mod_inventory_storage.item where holdingsrecordid = '" + holdingsId + "' ");
				    while (itemResultSet.next()) {
				    	String barcode = itemResultSet.getString("barcode");
				    	String type = itemResultSet.getString("materialTypeId"); 
				    	String itemLocation = lookupTable.get(itemResultSet.getString("permanentLocationId"));
				    	DataField itemField = MarcFactory.newInstance().newDataField();
				    	itemField.setTag("097");
				    	Subfield itemType = MarcFactory.newInstance().newSubfield('t', lookupTable.get(type));
				    	Subfield barcodeSubField = MarcFactory.newInstance().newSubfield('i',barcode);
				    	Subfield itemLocationSubfield = MarcFactory.newInstance().newSubfield('k', itemLocation);
				    	itemField.addSubfield(itemType);
				    	itemField.addSubfield(itemLocationSubfield);
				    	itemField.addSubfield(barcodeSubField);

				    	record.addVariableField(itemField);
				    }

		    	}
		    	
		    	writer.write(record);
		    	in.close();
		    }
		    catch(Exception e) {
		    	logger.fatal(e.getLocalizedMessage());
		    	logger.fatal(rawMARC);
		    	continue;
		    }
		}
		conn.close();
		writer.close();
		System.out.println("finished export");

	}
	
	public static HashMap<String,String> lookupReferenceValues( List<String> lookupTables ) throws ClassNotFoundException, SQLException {
		Map<String, String> lookUpTable = new HashMap<String,String>();
		Class.forName("org.postgresql.Driver");
		String url = "jdbc:postgresql://" + dburl + ":" + dbport + "/" + dbname + "?user=" + dbuser + "&password=" + dbpassword;
		Connection conn = DriverManager.getConnection(url);
		Iterator<String> lookupTablesIterator = lookupTables.iterator();
		while (lookupTablesIterator.hasNext()) {
			String table = lookupTablesIterator.next();
			Statement dbStatement = conn.createStatement();
			ResultSet queryResults = dbStatement.executeQuery("select jsonb->>'id' as id, jsonb->>'name' as name from " + table );
			while (queryResults.next()) {
				String id = queryResults.getString("id");
				String name = queryResults.getString("name");
				lookUpTable.put( id,name);		
			}
		}
		conn.close();	
		return (HashMap<String, String>) lookUpTable;
	}
}
