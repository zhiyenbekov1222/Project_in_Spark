from spark_data_upload import *
#run1 = data_etl()
#df_updated_restaurants = run1.rest_df_updated()
# df_updated_restaurants.count()
import time
run2 = weather_data()
run2.join_and_save()