import unittest
from unittest.mock import patch, MagicMock
import yaml
import os
import requests
from pyspark.sql import SparkSession
from spark_data_upload import rest_data_etl, load_config 

class TestRestDataETL(unittest.TestCase):
    
    @patch('spark_data_upload.load_config')  
    def test_load_config(self, mock_load_config):
        mock_load_config.return_value = {'url': 'https://api.opencagedata.com/geocode/v1/json', 'api': '31a70cdba08e483589c87f554c3078e'}
        
        config = load_config('credentials.yaml')
        
        mock_load_config.assert_called_once_with('credentials.yaml')
        
        self.assertEqual(config['url'], 'https://api.opencagedata.com/geocode/v1/json')
        self.assertEqual(config['api'], '31a70cdba08e483589c87f554c3078e')
    
    @patch('os.listdir')  
    @patch('spark_data_upload.spark.read.csv') 
    def test_get_rest_data(self, mock_spark_read, mock_os_listdir):

        mock_os_listdir.return_value = ['file1.csv', 'file2.csv']
        
        mock_df = MagicMock()  
        mock_spark_read.return_value = mock_df
        
        etl = rest_data_etl()
        
        result_df = etl.get_rest_data()

        mock_spark_read.assert_any_call('dummy_path/file1.csv', header=True, inferSchema=True)
        mock_spark_read.assert_any_call('dummy_path/file2.csv', header=True, inferSchema=True)
        
        self.assertEqual(result_df, mock_df)
    
    @patch('spark_data_upload.requests.get') 
    def test_get_lat_lon_from_address(self, mock_requests_get):
    
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'results': [{'geometry': {'lat': 40.7128, 'lng': -74.0060}}]
        }
        mock_requests_get.return_value = mock_response
        
        etl = rest_data_etl(api_key='test-api-key')
        
        lat, lon = etl.get_lat_lon_from_address('New York')
        
        mock_requests_get.assert_called_once_with(
            'https://api.opencagedata.com/geocode/v1/json',
            params={'q': 'New York', 'key': 'test-api-key', 'no_annotations': 1, 'limit': 1}
        )
        
        self.assertEqual(lat, 40.7128)
        self.assertEqual(lon, -74.0060)

    @patch('spark_data_upload.rest_data_etl.get_rest_data')
    @patch('spark_data_upload.rest_data_etl.get_lat_lon_data')
    def test_get_df_filled(self, mock_get_lat_lon_data, mock_get_rest_data):

        mock_get_rest_data.return_value = MagicMock()  
        mock_get_lat_lon_data.return_value = {1: (40.7128, -74.0060)}  # testing lat/lon data
        
        etl = rest_data_etl()
        
        filled_df = etl.get_df_filled()
        
        self.assertIsNotNone(filled_df)

    @patch('spark_data_upload.rest_data_etl.rest_df_updated')
    @patch('spark_data_upload.rest_data_etl.get_df_filled')
    def test_rest_df_updated(self, mock_get_df_filled, mock_rest_df_updated):
 
        mock_get_df_filled.return_value = MagicMock() 
        
        etl = rest_data_etl()

        updated_df = etl.rest_df_updated()
        
        self.assertIsNotNone(updated_df)

    @patch('spark_data_upload.rest_data_etl.rest_df_updated')
    def test_get_rest_geohash(self, mock_rest_df_updated):
        
        mock_rest_df_updated.return_value = MagicMock()  
        
        etl = rest_data_etl()
        
        geohash_df = etl.get_rest_geohash()
        
        self.assertIsNotNone(geohash_df)

if __name__ == '__main__':
    unittest.main()
