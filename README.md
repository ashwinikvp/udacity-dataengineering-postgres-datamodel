# Project 1 : Data Modeling with Postgres

This porject creates a postgres database for a music application called "Sparkify". The main object of this project to is model the data for running the optimised queries for song play analysis.
The song related details and user log details are stored in JSON format. The data from these files are fetched and loaded into star and dimension table.

# Schema design

In the current project, schema design could be an example of "Start schema".

**Dimension Tables are: users, songs, artist and time.**

**Fact table : Songplays**

Fact tables usaually contains facts or metrics about the business.The fact table consists of two categories of columns.
One category of columns containing foreign keys to the dimension tables.
Example in the current project would be, user_id(Foreign key) referring to primary key of users table.  
Second category of columns which contain the facts. Example could be location, user agent in the "Songplays" table.

Dimension table provides more details of the products.   
In the current example, details about songs, artist, users etc. i.e. When the song was created? who created the song? who is using the song? 

 ![](songplays_schema.png?raw=true)

# Project structure  

>songplays_schema.PNG: Schema design of the project.  

>sql_queries.py : Contain the queries for creating facts and dimension table. 

>create_tables.py: Python script which users sql_queries for creating and dropping tables by connecting to database 

>etl.py : ETL pipleline for loading the data from JSON files to star schema, with no bulk copy.  

>etlbulk.py:  ETL pipleline for loading the data from JSON files to star schema, with bulk copy.  


# Song Play example queries
1. Count number of Paid and Free users.  
   Select COUNT(*) from songplays GROUP BY level  
   


2. Total songs played by each artist(to fetch the artist name, it can be JOINED with artists table)  
   Select COUNT(*) from songplays GROUP BY artist_id 
   

3. Total times a song played  
   Select COUNT(*) from songplays GROUP BY song_id  
   

4. Songs played in particular location  
   Select COUNT(*) from songplays GROUP BY location  
   

5. Total songs played, where song was made in a particular year or each year.  
   Select COUNT(*) from songplays INNER JOIN songs on songplays.song_id = songs.song_id WHERE songs.song_year=2000
   Select COUNT(*) from songplays INNER JOIN songs on songplays.song_id = songs.song_id GROUP BY songs.song_year








