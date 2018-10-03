import boto3
import urllib.parse

glue = boto3.client('glue')
    
def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print(key)
    #i am using the name of the S3 notification to indicate if it is finding .zip suffix or .tgz/.tar.gz 
    triggerid = urllib.parse.unquote_plus(event['Records'][0]['s3']['configurationId'], encoding='utf-8')
    print(triggerid)
    #if zipfile, triggid = 'zip'    
    destbucket = 'YOURDESTINATIONBUCKETHERE'
    
    try:
        newJobRun = glue.start_job_run(
            JobName = 'dupe',
            Arguments = {
                '--bucket':bucket,
                '--key':key,
                '--destbucket':destbucket,
                '--filetype':triggerid
            }
            )
        print("Successfully created unzip job")    
        return key  
    except Exception as e:
        print(e)
        print('Error starting unzip job for' + key)
        raise e       
