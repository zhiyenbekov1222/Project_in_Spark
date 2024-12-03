'''
The purpose of this project is to be familiarize you with the basics of the open source distributed processing system for big data workloads. In addition to getting acquainted with the key components, architecture, and various applications of Spark, the project will discover the wealth of operations Spark offers, techniques about extract, transform, load (ETL), and the sets of APIs available in Spark. Apply the knowledge earned from the Spark lesson with the goal of building an understanding of how to improve the efficiency of Spark applications.
Running time approximately
'''

from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import col, udf
import geohash2 as geohash
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, LongType
from tqdm import tqdm
from IPython.display import clear_output
import os
import requests
import yaml


def load_config(file_path='credentials.yaml'):
    '''
        This method returns the credentials for API request
    '''
    with open(file_path, 'r') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    return config

spark = SparkSession.builder \
    .appName("CSVExample") \
    .getOrCreate()

path = '/home/jovyan/work/restaurant_csv'
csv_files = [file for file in os.listdir(path) if file.endswith('.csv')]
files_weather = [file for file in os.listdir('Weathers') if file.startswith('weather')]

config = load_config()
# API Key for OpenCage Geocoding API
url=config['url']
API_KEY=config['api']



class rest_data_etl:
    '''
    This object prepares the restaurant data for merge with weather dataset.
    '''
    def __init__(self, path = path, api_key = API_KEY):
        '''
        The constructor of class data_etl
        '''
        self.path = path
        self.api_key = api_key

    def get_rest_data(self):
        '''
        This method returns the restaurant data
        '''
        df_restaurants = None
        for file in csv_files:
            df_temp = spark.read.csv(self.path + '/' + file, header=True, inferSchema=True)
            if df_restaurants is None:
                df_restaurants = df_temp
            else:
                df_restaurants = df_restaurants.union(df_temp)
        return df_restaurants

    def get_null_rows(self):
        '''
        This method returns spark dataframe with missing latitude and longitude from the restaurant data
        '''
        df_null_lat_lon = self.get_rest_data().filter(col("lat").isNull() | col("lng").isNull())
        return df_null_lat_lon

    def get_lat_lon_from_address(self, address):
        '''
        This method parses the latitude and longitude data using the OpenCage API
        '''
        base_url = "https://api.opencagedata.com/geocode/v1/json"
        params = {
            'q': address,
            'key': self.api_key,
            'no_annotations': 1,  # Skip unnecessary annotations
            'limit': 1            # Only get the top result
        }
        try:
            response = requests.get(base_url, params=params)
            data = response.json()
            if data['results']:
                lat = data['results'][0]['geometry']['lat']
                lon = data['results'][0]['geometry']['lng']
                return lat, lon
            else:
                return None, None
        except Exception as e:
            print(f"Error occurred while calling OpenCage API: {e}")
            return None, None

    def get_lat_lon_data(self):
        '''
        This method fetches latitude and longitude for the missing entries
        '''
        df_null_rows = self.get_null_rows()
        missing_cities = df_null_rows.select("city", "id").distinct().rdd.flatMap(lambda x: x).collect()
        
        lat_lon_data = {}
        for idx in range(0, len(missing_cities), 2):
            city = missing_cities[idx*2]
            id   = missing_cities[idx*2 + 1]
            lat, lon = self.get_lat_lon_from_address(city)
            lat_lon_data[id] = (lat, lon)

        return lat_lon_data

    def get_df_filled(self):
        '''
        This method fills missing latitude and longitude for restaurants using OpenCage API
        '''
        # Fetch missing latitudes and longitudes for cities
        lat_lon_data = self.get_lat_lon_data()

        # Create a DataFrame with city and corresponding lat/lon
        lat_lon_df = spark.createDataFrame(
            [(id, lat, lon) for id, (lat, lon) in lat_lon_data.items()],
            StructType([
                StructField("id", LongType(), True),
                StructField("lat", DoubleType(), True),
                StructField("lng", DoubleType(), True)
            ])
        )
        return lat_lon_df
        # Joining the restaurant data with the fetched lat/lon data
        df_restaurants = self.get_rest_data()
        df_filled = df_restaurants.join(lat_lon_df, on="city", how="left")

        return df_filled

    def rest_df_updated(self):
        '''
        This method updates the restaurant dataframe
        '''
        df_restaurants_alias = self.get_rest_data().alias("restaurants")
        df_updated_alias     = self.get_df_filled().alias("updated")
        
        df_joined = df_restaurants_alias.join(df_updated_alias, on='id', how='left')
        
        df_updated_restaurants = df_joined.select(
            'id', 
            df_restaurants_alias['franchise_id'],
            df_restaurants_alias['franchise_name'],
            df_restaurants_alias['restaurant_franchise_id'],
            df_restaurants_alias['country'],
            df_restaurants_alias['city'],
            # Use coalesce to get lat and lon from df_updated (if exists) or fallback to df_restaurants
            functions.coalesce(df_restaurants_alias['lat'], df_updated_alias['lat']).alias('lat'),
            functions.coalesce(df_restaurants_alias['lng'], df_updated_alias['lng']).alias('lng')
        )

        return df_updated_restaurants


    def get_rest_geohash(self):
        '''
        This method transforms geohash and adds one more feature
        '''
        df_updated_restaurants = self.rest_df_updated()
            
        def generate_geohash(lat, lon):
            return geohash.encode(lat, lon, precision=7)
            
        geohash_udf = udf(generate_geohash, StringType())
        
        # Apply the UDF to the DataFrame
        df_with_geohash = df_updated_restaurants.withColumn("geohash", geohash_udf(col("lat"), col("lng")))
        print("Object for Restaurant dataset finished.")
        return df_with_geohash


class weather_data():
    '''
    This object prepares the weather data for merge with restaurant dataset.
    '''
    def __init__(self, path = path, api_key = API_KEY):
        '''
        The constructor of class data_etl
        '''
        self.path = path
        self.api_key = api_key

    def get_weather_data(self):
        '''
        This method prepares and returns weather dataset in the format spark dataframe. It accumulates each part of data into empty dataframe.
        '''
        df_w_all = None
        for file_w in tqdm(files_weather):
            path_1 = 'Weathers/' + file_w
            path_2 = path_1 + '/' + os.listdir(path_1)[1]
            path_3 = path_2 + '/' + os.listdir(path_2)[1]
            days = [days for days in os.listdir(path_3) if days.startswith('day')]
            for day in days:
                path_4 = path_3 + '/' + day
                
                parquets = [parq for parq in os.listdir(path_4) if parq.endswith('.parquet')]
                for parquet in parquets:
                    df_w_temp = spark.read.parquet(path_4 + '/' + parquet)
                    
                    if df_w_all is None:
                        df_w_all = df_w_temp
                    else:
                        df_w_all = df_w_all.union(df_w_temp)

        return df_w_all

    def get_weather_hash_data(self):
        '''
        This method transforms geohash and adds one more feature
        '''
        def generate_geohash(lat, lon):
            return geohash.encode(lat, lon, precision=7)
        geohash_udf = udf(generate_geohash, StringType())
        df_w_geohash_all = self.get_weather_data().withColumn("geohash", geohash_udf(col("lat"), col("lng")))

        return df_w_geohash_all

    def join_and_save(self):
        '''
        This method joins two dataframes weather and restaurants. Then it saves data by partition in a parquet format.
        The large data is devided into 10 part.
        '''
        
        run1 = rest_data_etl()
        df_rest_all = self.get_weather_hash_data().withColumn("p", (functions.monotonically_increasing_id() % 10))
        df_with_geohash = run1.get_rest_geohash()
        # Now repartition and filter based on the new column `p`
        df_rest_all = df_rest_all.repartition(10)
        
        partitions = [df_rest_all.filter(f"p = {i}") for i in range(10)]
        #df1 = partitions[0]
        for idx, df1 in tqdm(enumerate(partitions)):
            #clear_output(wait=True)
            print(f'Iteration {idx}. Data loading...')
            df1 = df1 \
            .withColumnRenamed("lat", "lat_1") \
            .withColumnRenamed("lng", "lng_1") \
            .withColumnRenamed("geohash", "geohash_1")
            
            df_joined = df1.join(
                df_with_geohash,           # The second DataFrame
                df1["geohash_1"] == df_with_geohash["geohash"],  # The condition for the join
                "left"                      # The type of join (left join in this case)
            )
        
            df_joined.write.mode("overwrite").parquet(f'datasets/df_joined_{idx}')
            print(f'The parquet {idx} has succesfully recorded. ')

        
                
