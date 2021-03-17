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
For the daily VuFind incremental update we were using the FOLIO OAI/PMH (marc21) module and the scripts built into VuFind. 
