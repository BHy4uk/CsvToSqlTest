# Taxi Trips Database

This repository contains scripts and information for setting up and querying the Taxi Trips database.

## Database Setup

1. **Database Creation**: Execute the following SQL command to create the database:
    ```sql
    CREATE DATABASE TaxiTripsDB;
    ```
2. **Update appsetting.json file with your DB connection configuration**
3. **TaxiTrips Table Creation**: Execute the following SQL command to create the `TaxiTrips` table:
    ```sql
    CREATE TABLE TaxiTrips (
        tpep_pickup_datetime DATETIME,
        tpep_dropoff_datetime DATETIME,
        passenger_count INT,
        trip_distance FLOAT,
        store_and_fwd_flag NVARCHAR(3),
        PULocationID INT,
        DOLocationID INT,
        fare_amount DECIMAL(10, 2),
        tip_amount DECIMAL(10, 2)
    );
    ```

## Data Overview

- **Number of Rows**: 29889
- **Number of Duplicate Rows**: 111
- **You can find file with dublicates YourProjectLocation\CsvToSqlTest-master\CsvToSqlTest-master\CsvToSqlTest\bin\Debug\net8.0**

## Example Queries

1. **Query 1**: Find the top pickup location with the highest average tip amount:
    ```sql
    SELECT TOP 1 PULocationID, AVG(tip_amount) AS avg_tip_amount
    FROM TaxiTrips
    GROUP BY PULocationID
    ORDER BY avg_tip_amount DESC;
    ```

2. **Query 2**: Retrieve the top 100 records ordered by trip distance:
    ```sql
    SELECT TOP 100 *
    FROM TaxiTrips
    ORDER BY trip_distance DESC;
    ```

3. **Query 3**: Retrieve the top 100 records ordered by trip duration:
    ```sql
    SELECT TOP 100 *
    FROM TaxiTrips
    ORDER BY DATEDIFF(minute, tpep_pickup_datetime, tpep_dropoff_datetime) DESC;
    ```

4. **Query 4**: Search for records with a pickup location ID containing '143':
    ```sql
    SELECT *
    FROM TaxiTrips
    WHERE PULocationID LIKE '%143%';
    ```
## What to change if it will be 10GB CSV input file

### Streaming Processing
Implement streaming processing techniques to read the CSV file in chunks rather than loading the entire file into memory at once. This approach will reduce memory usage and allow processing of large files efficiently.
### Batch Processing
Implement batch processing to handle data in smaller batches rather than processing all records at once. This approach will help to manage memory usage and improves scalability when dealing with large datasets.
