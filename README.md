
# Spark Practice!

Date completion: 02.12.2024

This project is a part of the [Data Engineering 2024 TechOrda](https://university.epam.com/myLearning/path?rootId=15377596) at [Epam University](https://campus.epam.kz/ru). 

#### Project Status: [Completed]

## Project Intro/Objective
The purpose of this project is to be familiarize you with the basics of the open source distributed processing system for big data workloads. In addition to getting acquainted with the key components, architecture, and various applications of Spark, the project will discover the wealth of operations Spark offers, techniques about extract, transform, load (ETL), and the sets of APIs available in Spark. Apply the knowledge earned from the Spark lesson with the goal of building an understanding of how to improve the efficiency of Spark applications. 


### Methods Used
* Data Cleaning
* API Integration
* Geospatial Analysis
* Data Transformation
* Data Merging
* Data Aggregation
* Data Storage
* ETL (Extract, Transform, Load)
* Idempotent Operations

### Technologies
* Python
* Apache Spark
* PySpark
* OpenCage Geocoding API
* Geohash
* Parquet
* Docker
* REST API
* Jupyter Notebooks
* SQL
* JSON (for API data format)

## Project Description
This is a final practical task of Spark Lesson. The step and task is provided below.

## PREREQUISITES
 * Install Spark locally using one of the methods described [here](https://spark.apache.org/downloads.html) or in Docker.
 * Create a Spark ETL job to read data from a local storage. You can find the data in the Spark Practice—Dataset file on the page with the task description.

## TASK
 * Load dataset *Restaurants* and show available rows and collumns.
 * Check restaurant data for incorrect (null) values (latitude and longitude).
 * For incorrect values, map latitude and longitude from the [OpenCage Geocoding API](https://opencagedata.com/api) in a job via the REST API.
 * Generate a geohash by latitude and longitude using a geohash library like geohash-java. Your geohash should be four characters long and placed in an extra column.
 * Load dataset *Weather* and show available rows and collumns. Check for missing values (latitude and longitude).
 * Generate geohash for weather dataset
 * Left-join weather and restaurant data using the four-character geohash. Make sure to avoid data multiplication and keep your job idempotent.
 * Store the enriched data (i.e., the joined data with all the fields from both datasets) in the local file system, preserving data partitioning in the parquet format.


## Featured Notebooks/Analysis/Deliverables
* [spark_data_upload.py]([link](https://github.com/zhiyenbekov1222/Project_in_Spark/blob/main/spark_data_upload.py)![image](https://github.com/user-attachments/assets/f44a2280-33c6-4120-8d35-bd9135f0ee44)
)
* [test_spark.py]([link](https://github.com/zhiyenbekov1222/Project_in_Spark/blob/main/test_spark.py)![Uploading image.png…]()
)


## Contributing owner

**Epam Big Data Engineering student : [Zhalgas Zhiyenbekov](https://github.com/zhiyenbekov1222?tab=repositories))**

## Contact
* LinkedIn [Zhalgas Zhiyenbekov](http://c4sf.me/slack](https://www.linkedin.com/in/zhiyenbekov/ ).  
