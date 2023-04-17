import redshift_connector

conn=redshift_connector.connect(
     host='project-run-cluster.cqd7otjyrwat.ap-south-1.redshift.amazonaws.com',
     database='dev',
     port=5439,
     user='awsuser',
     password='Aws12345'
  )
  
cursor=conn.cursor()

cursor.execute('drop table song_data;')
cursor.execute('drop table log_data;')
cursor.execute('drop table song;')
print("Tables Dropped")

conn.commit()
conn.close()