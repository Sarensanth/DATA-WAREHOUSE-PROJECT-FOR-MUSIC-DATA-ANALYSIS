import boto3
import redshift_connector
import csv

akey=''
skey=''
region='ap-south-1'

# host='project-run-cluster.cqd7otjyrwat.ap-south-1.redshift.amazonaws.com'
# port=5439
# user='awsuser'
# password='Aws12345'
# db='dev'

schema='public'
bucketname='project-run-bucket'
bucketfiles=[]

s3=boto3.client('s3', aws_access_key_id=akey, aws_secret_access_key=skey, 
                  region_name=region)

conn=redshift_connector.connect(
     host='project-run-cluster.cqd7otjyrwat.ap-south-1.redshift.amazonaws.com',
     database='dev',
     port=5439,
     user='awsuser',
     password='Aws12345'
  )
cursor=conn.cursor()

s3files=s3.list_objects_v2(Bucket=bucketname)

for obj in s3files['Contents']:
    if obj['Key'][-4:] == '.csv':
        bucketfiles.append(obj['Key'])




for filename in bucketfiles:
    table_name=filename.split('.')[0]
    with open(filename, 'r') as f:
        first_line=f.readline()
    columns=first_line.strip().split(',')
    create_table_query = f"CREATE TABLE {table_name} ({','.join([f'{col} VARCHAR(400)' for col in columns])});"
    cursor.execute(create_table_query)
    conn.commit()
    print(str(table_name)+" table created")


for filename in bucketfiles:
    table_name=filename.split('.')[0]
    s3_path = f's3://{bucketname}/{filename}'
    copy_query = f"COPY {schema}.{table_name} FROM '{s3_path}' CREDENTIALS 'aws_access_key_id={akey};aws_secret_access_key={skey}' DELIMITER ',' IGNOREHEADER 1;"
    cursor.execute(copy_query)
    conn.commit()
    print(str(table_name)+" table data copied")

cursor.close()
conn.close()





