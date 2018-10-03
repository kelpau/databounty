import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
 
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','bucket','key'],)
 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

import boto3
import zipfile
import io
from contextlib import closing

s3 = boto3.client('s3')
s3r = boto3.resource('s3')

bucket = args["bucket"]
key = args["key"]

obj = s3r.Object(
    bucket_name=bucket,
    key=key
)

buffer = io.BytesIO(obj.get()["Body"].read())
z = zipfile.ZipFile(buffer)
list = z.namelist()
for filerr in list:
    print(filerr)
    y=z.open(filerr)
    arcname = key + filerr
    x = io.BytesIO(y.read())
    s3.upload_fileobj(x, 'ziptests', arcname)
    y.close()
print(list)

 
job.commit()