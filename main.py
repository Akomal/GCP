import os
from google.cloud import storage
os.environ['GOOGLE_APPLICATION_CREDENTIALS']='json file_path'
storage_client= storage.Client()

#create new bucket
bucket_name='name'

bucket=storage_client.bucket(bucket_name)

#bucket=storage_client.create_bucket(bucket)

#print bucket details
#vars(bucket)

#Access a bucket

my_bucket=storage_client.get_bucket('name')


#upload files
def upload_to_bucket(blob_name,file_path,bucket_name):
    bucket=storage_client.get_bucket(bucket_name)
    blob=bucket.blob(blob_name)
    blob.upload_from_filename(file_path)
    return True

file_path=r'/home/...'

upload_to_bucket('allergies',os.path.join(file_path,'allergies.csv'),bucket_name)

