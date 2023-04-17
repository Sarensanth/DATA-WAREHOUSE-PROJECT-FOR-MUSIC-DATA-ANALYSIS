import redshift_connector
import pandas as pd

conn=redshift_connector.connect(
     host='project-run-cluster.cqd7otjyrwat.ap-south-1.redshift.amazonaws.com',
     database='dev',
     port=5439,
     user='awsuser',
     password='Aws12345'
  )

# print("Connection made")  
cursor=conn.cursor()

print("song_data table")
cursor.execute('SELECT * FROM song_data LIMIT 100')
song=pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
print(song)

print("log_data table")
cursor.execute('SELECT * FROM log_data LIMIT 100')
log=pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
print(log)


# cursor.execute('drop table song;')


cursor.execute(""" CREATE TABLE song AS SELECT 
                t.user_id, t.song_id, t.listen_count, 
                s.title, s.release, s.artist_name, s.year FROM log_data t 
                LEFT JOIN (SELECT DISTINCT song_id, title, release, artist_name, year FROM song_data) s 
                ON t.song_id = s.song_id""")

print("Most listened songs list")

cursor.execute("""SELECT title,song_id,artist_name, 
            COUNT(listen_count) as total_listen_count FROM song 
            GROUP BY title,song_id,artist_name 
            ORDER BY total_listen_count DESC LIMIT 100;""")

popular=pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
print(popular)


conn.commit()
conn.close()
                
