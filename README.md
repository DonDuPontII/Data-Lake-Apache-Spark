## Project Documentation

1. Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.
    - The relational database *sparktifydb* is constructed where Sparkify data can be easily queried and is curated to what song users are listening to.
    - With the increase in users, migrating to a data lake allows Sparktify the opportunities to not only continue increasing scalability, but also:
        - Make use of various data values, formats, and structures
        - Apply advanced analytics
        - Save financially through columnar storage using parquet

2. State and justify your database schema design and ETL pipeline.
    - Utilizing a star schema, a single fact table *songplays* focuses on data associated with song plays aligning to the primary analysis of interest. 
    - Four dimension tables contain Sparktify characteristics that can join to the *songplays* fact table to uncover insights. 
    - The dimensions tables included: 
        - *Users* (users in the app) 
        - *Songs* (songs reflected in the database) 
        - *Artists* (artists reflected in the database) 
        - *Time* (date and time units reflecting when songs were played) 
    - Due to the rudimentary dimensions/characteristics, there's no need to build a snowflake schema, which would involve a multidimensional layout. 
        - A use case for a snowflake schema: adding a user's most listened-to artist dimension table that joins to the user dimension table.
    - Utilizing fact and dimension tables work together to form organized data models that are intuitive to query.


### Files contained in the repository
1. ETL.py
    - Reads in Sparktify files from Amazon S3
    - Extracts column headers and data for each table
    - Writes tables back to a customizable S3 bucket
2. Dl.cfg
    - A space to populate your AWS access key ID and secret key

### How to run the process
1. Create an EMR cluster
2. Create an Amazon S3 bucket where tables can be written to 
3. Populate dl.cfg parameters
4. Update output_data variable to reflect your created S3 bucket
4. Execute ETL.py
