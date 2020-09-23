## MARC Export Scripts for FOLIO - WORK IN PROGRESS

This code is a work in progress.  It does produces a .json file that successfully imports into VuFind.  

Feedback/modifications are welcome!

### export-new.py - uses database queries to:
* In batches of 300K
* Gets a list of IDs for instances that are not blocked from discovery and state of the MARC record is 'ACTUAL'
* Using those IDs it retreives the MARC records
* Attempts to lookup holdings and items for the instance and adds 998 and 097 fields to the record
* Writes the records to a file (the python version writes MARC JSON)
* Attempts to filter out problem records (and logs them)


Notes:
For the VuFind incremental export we were using a script that used API calls to build the export file.  The most important API call being "/source-storage/source-records?updatedAfter=".
This works when 'thousands' of records were updated in one day...it does not work when there have been 150k records updated.  The module runs out of memory and returns a 500 error.  At Lehigh we are moving towards implementing the OAI-PMH module for our incremental updates.

