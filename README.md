# elasticscroll

## Install

Create env if it is not created.

```
virtualenv -p python3 ~/envs/elasticscroll
```

Activate env
```
source  ~/envs/elasticscroll/bin/activate
```

Install req

```
pip install -r requirements.txt
```


## Config

### Extract

```json
{
    "host": "19d7d779f8a502497d7eed2a5d035771.ap-southeast-2.aws.found.io",
    "port": 9243,
    "index": "patrick-labtrans",
    "size":1000,
    "query": {
        "match_all": {}
    },
    "sort": [
        "_doc"
    ],
    "s3.bucket":"elasticscroll"
}
```

### Query
```json
{
    "query": "select * from elastic",
    "database":"sampledb",
    "output": {
        "local": "output",
        "s3": "s3://elasticscroll"
    }
}
``` 

### Athena Setup

1. Go to Athena : https://console.aws.amazon.com/athena/
2. Create Table (Manually)
   *  Create a new database
   *  Put the name of the table
   *  Location of the folder where ou store files ( Ex : s3://elasticscroll/files/ )
   *  Click Next
   *  Chooser JSon Format 
   *  Click Next
   *  Create columns with names from json 
   *  Create next
   *  Create  table 
   
 
 Note:
 
 Choose different folder for upload files and query configuration
   

