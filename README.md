# CCNews

CCNews is a platform to analyze Common Crawl News data in batch processes to flag country references and store them in an Elasticsearch database. Kibana can then be used to query and visualize the data.  
http://www.ccnews.press:5601

## Directory Structure  
├── dep  
│   ├── ccnewsfiles.txt  
│   └── country-list.txt  
├── run_batch.sh  
└── src  
    └── newscrawl.py  

## Files  
*ccnewsfiles.txt: List of all WARC files to analyze from S3  
*country-list.txt: List of countries, abbreviations, aliases, lat/lon values  
*run_batch.sh: Script to run Spark job with correct configurations  
*newscrawl.py: PySpark code to analyze the data  

## Dependencies  
*os  
*re  
*json  
*elasticsearch  
*BeautifulSoup
