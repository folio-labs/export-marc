## MARC Export Scripts for FOLIO - WORK IN PROGRESS

This code is a work in progress.  It does produce a .mrc file that successfully imports into VuFind.

### What it does (using database queries)
* Gets a list of all instance IDs that are not blocked from discovery
* Using the intance ID it looks up the ID for the raw MARC record 
* Reads in the raw MARC record from the database
* Attempts to lookup holdings and items for the instance and adds 998 and 097 fields to the record
* Writes the records to a file

### What is does not do (yet)
* It is not configurable
* DB Info is hard-coded
* Tenant ID is hard-coded 
* Drops records due to encoding ?
* Doesn't perform paging or divide the records into multiple files
* ....things I haven't thought of yet


There are two versions in this repo:

### Java
Works on windows and debian
In my testing on Windows and Debian, encoding issues occur with about 2% of records.  If the ecoding issues cannot be resolved, one possible work-around is constructing the MARC using the JSON in the source record storage.

### Python
Works well on windows, however when tested on debian drops about 30% of records due to encoding issues. 
Written W/Python 3.7.3


