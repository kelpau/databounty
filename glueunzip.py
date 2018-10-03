import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
 
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','bucket','key','destbucket','filetype'],)
 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
import random
import boto3
import zipfile
import tarfile
import io
from contextlib import closing

s3 = boto3.client('s3')
s3r = boto3.resource('s3')

bucket = args["bucket"]
key = args["key"]
destbucket = args["destbucket"]
filetype = args["filetype"]

obj = s3r.Object(
    bucket_name=bucket,
    key=key
)

if filetype == 'zip':
    buffer = io.BytesIO(obj.get()["Body"].read())
    z = zipfile.ZipFile(buffer)
    list = z.namelist()
    itemcount = str(len(list))
    
    if len(list) < 100:
        samplelist = random.sample(list,10)
    elif len(list) < 1000:
        samplelist = random.sample(list,50)
    else:
        samplelist = random.sample(list,100)
    
    for filerr in samplelist:
        print(filerr)
        y=z.open(filerr)
        arcname = key + '/' + filerr
        x = io.BytesIO(y.read())
        s3.upload_fileobj(x, destbucket, arcname)
        y.close()
    print(list)
    
else:
    buffer = io.BytesIO(obj.get()["Body"].read())
    t = tarfile.open(fileobj=buffer)
    list = t.getnames()
    itemcount = str(len(list))
    
    if len(list) < 10:
        samplelist = list
    elif len(list) < 100:
        samplelist = random.sample(list,10)
    elif len(list) < 1000:
        samplelist = random.sample(list,50)
    else:
        samplelist = random.sample(list,100)
    
    for filerr in samplelist:
        print(filerr)
        y=t.extractfile(filerr)
        arcname = key + '/' + filerr
        x = io.BytesIO(y.read())
        s3.upload_fileobj(x, destbucket, arcname)
        y.close()
    print(list)    
    

obj.metadata.update({'itemcount':itemcount})
obj.copy_from(CopySource={'Bucket':bucket, 'Key':key}, Metadata=obj.metadata, MetadataDirective='REPLACE')
 
job.commit()
