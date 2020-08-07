## MARC Export Scripts for FOLIO - WORK IN PROGRESS

This code is a work in progress.  It does produce a .mrc file that successfully imports into VuFind.

Feedback/modifications are welcome!

### What it does (using database queries)
* Gets a list of all instance IDs that are not blocked from discovery
* The initial query that gets the instance IDs has a limit=100 parameter - for testing
* Using the intance ID it looks up the ID for the raw MARC record 
* Reads in the raw MARC record from the database
* Attempts to lookup holdings and items for the instance and adds 998 and 097 fields to the record
* Writes the records to a file (the python version writes MARC JSON)

### What is does not do (yet)
* It is not configurable
* DB Info is hard-coded
* Tenant ID is hard-coded 
* 998, 097 hard-coded
* ....things I haven't thought of yet


There are two versions in this repo:

### Java
* Works on windows and debian
* In my testing on Windows and Debian, encoding issues occur with about 2% of records.  If the ecoding issues cannot be resolved, one possible work-around is constructing the MARC using the JSON in the source record storage.

### Python
* Writes MARC json records into a file that VuFind is able to index
* Written W/Python 3.7.3
* There is a python version that does an incremental export


