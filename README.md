## Process Documentation

1. Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.
    - The relational database is constructed where Sparkify data can be easily queried from and is curated to what song users are listening to.
    - With the increase in users, migrating to a data lake allows Sparktify the opportunities to not only continue increasing scalability, but also:
        - make use of various data values, formats, and structures
        - apply advanced analytics
        - save financially through columnar storage using parquet


2. State and justify your database schema design and ETL pipeline.
    - Utilizing a star schema, there is a single fact table *songplays* focusing on data associated with song plays aligning to the primary analysis of interest. 
    - There are four dimension tables containing Sparktify characteristics that can easily be joined to the *songplays* fact table to uncover insights. 
    - The dimensions tables included: 
        - *users* (users in the app) 
        - *songs* (songs reflected in the database) 
        - *artists* (artists reflected in the database) 
        - *time* (date and time units of when songs were played) 
    - Due to the rudimentary dimensions/characteristics, there wasn’t a need to build out a snowflake schema, which would involve a multidimensional layout. 
        - For example, if data around a user’s most listened to artist began being collected, then building a secondary dimension off of the user dimension table would reflect the need for a snowflake schema. 
    - Utilizing fact and dimension tables work together to form organized data models that are intuitive to query off of.


### Files contianed in the repository
1. etl.py
    - Reads in Sparktify files from AWS S3
    - Extracts column headers and data for each table
    - Writes tables back to a customizable S3 bucket
2. dl.cfg
    - A space to populate your AWS access key ID and secret key

### How to run process
1. Create an EMR cluster
2. Create an AWS S3 bucket where tables can be written to 
3. Populate dl.cfg parameters
4. Update output_data variable to reflect your created S3 bucket
4. Execute etl.py