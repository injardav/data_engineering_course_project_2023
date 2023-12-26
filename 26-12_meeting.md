we met on Discord and :
- Explored what we have done to enrich the data. 
- Discussing the data schema and what challenges we have for better enrichment.
	==> Author not being unique when searching for via scholarly binaries, to get related attribute like affiliation. --> hence we might need use it if and only if they return a unique result. 
	==> dblp can't be used to retrive some of the data about the publishers and authors due to the authors not existed as exited in the paper or not exited at all.
- Discussing the Analytics layer and how we can see both of the DWH cubes and graphDB analytics.
- agree to implemented the next tasks:

* **Task 1**: Building the following Airflow Pipelines
      *The first Pipleline: 
        target: place the record in a persistant storage (stage area)in a file with avro Specification.
        things to do : 
          - Get a record from the data set.
          - Dropping papers with empty authors if is has an empty DOI.
          - Dropping papers with short titles less than 5 letters if is has an empty DOI.
          - Replace the ID feild with a uniqe key id feild.
          - Populate the General Category.
          - Dropping document with empty DOI

      * The second Pipeline: 
        target: 
	        - consume files from the staging area prepare the record to be ingested into the DB/DWH and The graphDB.
          things to do:
                - Consume the CrossrefAPI based on the paper DOI.
                - we have to manipulate the feild to be DB/graph friendly formatted.
 

       * thrid Pipeline : 
          target: 
         - populate the DW.
 
       * forth Pipeline:
           target 
         -populate the GraphDB.
   
 
* **Task 2**: Design the DWH and the graphDB Schemas based on the following : 
the schema dataset feilds:
id:
submitter:
authors:
title: 
comments:
journal-ref:
doi:  
report-no:
categories:
General_category: 
license:
versions:
update_date:
authors_parsed:
abstract 
reference-count: int
publisher:string
issue:string
license_start:date_time
license_url:string
license_content-version:string
license_delay:int
short-container-title:string
container-title:string.
type:   
is-referenced-by-count:int 
author[given,family,sequance]
reference[DOI,key,doi-asserted-by]
language:string
Links[]  
deposited: datetime
score:int 
ISSN[number, type]
article-number:string 

* **task3**: Implement the ELK stack to bring the search capability to the solution.
