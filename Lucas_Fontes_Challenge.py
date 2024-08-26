# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType, DoubleType

import requests, json

from pyspark.sql.functions import current_timestamp, date_format, col, split, regexp_replace, expr, from_utc_timestamp, from_unixtime, concat, lit, round as pyspark_round

from pyspark.sql.utils import AnalysisException

# COMMAND ----------

#Defining static variable
file_path = "/mnt/mercedes_challenge/ListOfCities.txt"
base_url = "https://api.openweathermap.org/data/2.5/weather?"
api_key = "f426e7da8f8efb1ff4add2574c330154"
list_of_cities = ["Lisbon", "Porto", "Braga", "Rio de Janeiro", "Berlim", "Paris", "Porto Alegre", "Bahia"]

# COMMAND ----------

class MercedesChallenge():

    def __init__(self, base_url, api_key):
        self.base_url = base_url
        self.api_key = api_key

    def connectToOpenWeather(self, city):
        """Connect to the API OpenWeatherMap to retrive climate data from the city .

        Args:
            city (str): City to be consulted

        Returns:
            dict: JSON containing the data or None in case of the city is not founded.
        """
        
        complete_url = f"{self.base_url}q={city}&appid={self.api_key}"
        response = requests.get(complete_url)

        if response.status_code == 200:
            result = response.json()
            return result
        else:
            print(f"Request failed with status code: {response.status_code}")
            dbutils.notebook.exit("Cancelling notebook execution given that there was no communication with API")


    def adding_city_files(self, city, file_path):
        """First call the function that checks if file exists, if true then add the city to the file in dbfs, if not creates it.
           The second if checks if the city is already added.

        Args:
            city (str): City to be consulted
            file_path (str): path of txt file 

        Returns:
            Add the data to the file or print a message informing that the city is already in the file.
        """
        # Check if file exists
        if self.file_exists(file_path):
            content = dbutils.fs.head(file_path)
            if city in content:
                print(f"The city '{city}' is already in the file.")
            else:
                print(f"The city '{city}' is not in the file. Adding it to the file...")
                new_content = content +  "\n" + city
                print(new_content)
                dbutils.fs.put(file_path, new_content, overwrite=True)
        else:
            print(f"File '{file_path}' does not exist. Creating it..")
            dbutils.fs.put(file_path, city, overwrite=True)

    @staticmethod
    def file_exists(file_path):
        """This function is called on adding_city_files to check if the file exists

        Args:
            file_path (str): path of txt file 

        Returns:
            boolean informing whether exists or not. 
        """
        try:
            # Extract directory and file_name
            dir_path = "/".join(file_path.split("/")[:-1])
            file_name = file_path.split("/")[-1]
            
            # List files in the directory
            files = dbutils.fs.ls(dir_path)
            
            # Check if file is in list
            for file in files:
                if file.name == file_name:
                    return True        
            return False
        except Exception as e:
            print(f"Error finding the file: {str(e)}")
            return False
        
    def read_dbfs_file(self,file_path):
        """Once the file is created and has cities in it, there's the need to read data from the file. This particular function exists because the challenge makes clear that the cities must be read from a config file.

        Args:
            file_path (str): path of txt file 

        Returns:
            list: contain the cities that we are going to pull data from API 
        """

        files = dbutils.fs.head("/mnt/mercedes_challenge/ListOfCities.txt")
        lines = files.splitlines()
        cities = []

        for element in lines:
            situation = value.connectToOpenWeather(element)
            cities.append(element)

        return cities
    
    def json_to_spark(self, json_data):
        """This function creates the spark dataframe based on the JSON generated from the API request. The if checks if the data is a dict. The code also calls the function schema that is, basically, a struct that is used to defined the types of the columns for the spark dataframe.

        Args:
            json_data: the data pulled from the API 

        Returns:
            dataframe: dataframe with the information of the cities.
        """
        data = json_data
        schema = self.schema()
        if isinstance(data, dict):
            df = spark.createDataFrame([data],schema)
            return df
        else:
            print("JSON is not a dict. Exiting function....")
            return "Json is not a dict"

    def schema(self):
        """This function is called on json_to_spark to help define the schema of the data pulled from the API. 

        Args:
            -

        Returns:
            A schema 
        """
        schema = StructType([
    StructField("coord", StructType([
        StructField("lon", StringType(), True),
        StructField("lat", StringType(), True)
    ]), True),
    StructField("weather", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("main", StringType(), True),
        StructField("description", StringType(), True),
        StructField("icon", StringType(), True)
    ])), True),
    StructField("base", StringType(), True),
    StructField("main", StructType([
        StructField("temp", DoubleType(), True),
        StructField("feels_like", DoubleType(), True),
        StructField("temp_min", DoubleType(), True),
        StructField("temp_max", DoubleType(), True),
        StructField("pressure", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("sea_level", IntegerType(), True),
        StructField("grnd_level", IntegerType(), True)
    ]), True),
    StructField("visibility", IntegerType(), True),
    StructField("wind", StructType([
        StructField("speed", DoubleType(), True),
        StructField("deg", IntegerType(), True),
        StructField("gust", FloatType(), True)  
    ]), True),
    StructField("clouds", StructType([
        StructField("all", IntegerType(), True)
    ]), True),
    StructField("dt", IntegerType(), True),
    StructField("sys", StructType([
    StructField("type", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("country", StringType(), True),
            StructField("sunrise", IntegerType(), True),
            StructField("sunset", IntegerType(), True)
        ]), True),
        StructField("timezone", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("cod", IntegerType(), True)
])
        return schema
    
    def write_layer(self, result_df, type_layer):
        """This function writes the data into the table on the respective layer following the Medallion Architecture principle. It's a function that can be used for any layer. The spark.catalog.tableExists checks if the table already exists, if not it is created, otherwise it checks if the current day has already been added by using 2 keys: the date and the city. If both exists the data has already been added, if not append the information on the layer.

        Args:
            result_df: Dataframe to be written on table
            type_layer: layer following Medallion Architecture principles

        Returns:
            data written on the table
        """

        if not spark.catalog.tableExists(f"default.{type_layer}_weather_data"):
            print(f"Table not exists, creating it...")
            result_df.write.format("delta").option("OverwriteSchema", "True").mode("overwrite").saveAsTable(f"default.{type_layer}_weather_data")
        else:
            data_written = [row[0] for row in spark.sql(f"SELECT CAST(`current_time` as DATE) FROM default.{type_layer}_weather_data").collect()]
            data_written_formated = [date.strftime("%Y-%m-%d") for date in data_written]
            data_to_be_written = result_df.select("current_time").distinct().collect()[0][0]

            cities_written = [row[0] for row in spark.sql(f"SELECT DISTINCT `name` FROM default.{type_layer}_weather_data order by `name` ").collect()]
            cities_to_be_written = result_df.select("Name").distinct().sort("Name").collect()[0][0]

            if (data_to_be_written in data_written_formated) and (cities_to_be_written in cities_written):
                print("This day has already been added to the table")
                pass
            else:
                result_df.write.format("delta").option("OverwriteSchema", "True").mode("append").saveAsTable(f"default.{type_layer}_weather_data")

    def pull_data_from_layer(self, db_name, table_name):
        """This function selects the data from the layer

        Args:
            db_name: name of the database
            table_name: name of the table

        Returns:
            dataframe: dataframe with the information of the cities
        """
        if not spark.catalog.tableExists(f"{db_name}.{table_name}"):
            print(f"Table {db_name}.{table_name} does not exists.")
        else:
            data_pulled = spark.sql(f"SELECT * FROM {db_name}.{table_name}")  
            return data_pulled

    
    def column_pruning(self, silver_data):
        
        """This function selects only the necessary columns and applies regexp to remove [] from 2 columns. The raw table has columns as arrays and use explode() is not the best option, because in large tables can cause spill. 

        Args:
            silver_data: Data pulled from the bronze layer.

        Returns:
            dataframe: dataframe only with the columns selected
        """

        silver_data_pruning = silver_data.select(
                                                 col('Name'),
                                                 col('weather.main').alias("Main_weather").cast(StringType()),
                                                 col('weather.description').alias("Weather_description").cast(StringType()),
                                                 col('main.temp').alias("Temperature"),
                                                 col('main.feels_like').alias("Temperature_feels_like"),
                                                 col('main.temp_min').alias("Minimal_Temperature"),
                                                 col('main.temp_max').alias("Maximal_Temperature"),
                                                 col('main.humidity').alias("Humidity"),
                                                 col('wind.speed').alias("Wind_Speed"),
                                                 col('timezone'),
                                                 col('current_time')
                                                )
        
        #Using regexp_replace to remove the [] from these 2 columns 
        silver_data_pruning = silver_data_pruning.withColumn("Main_weather", regexp_replace("Main_weather", r'[\[\]]', ''))\
                                                 .withColumn("Weather_description", regexp_replace("Weather_description", r'[\[\]]', ''))
                                                 

        return silver_data_pruning


    def convert_kelvin_to_celsius(self, silver_data_filtered):
        """This function converts data from the temperature columns that are in kelvin to celsius.

        Args:
            data filtered from the column_pruning function

        Returns:
            dataframe: dataframe with temperature columns converted to celsius scale. 
        """
        columns_kelvin = ["Temperature", "Temperature_feels_like", "Minimal_Temperature", "Maximal_Temperature"]

        for column in columns_kelvin:
            silver_data_filtered = silver_data_filtered.withColumn(
                column, 
                expr(f"{column} - 273.15")
            )

        return silver_data_filtered
    
    def convert_seconds_to_timezone(self, df):

        """This function converts the timezone column in seconds to the format UTC-3, for example. 

        Args:
            df: dataframe to be converted

        Returns:
            dataframe: dataframe converted
        """

        df_transformed_timezone = df.withColumn("timezone",concat(expr("CASE WHEN timezone < 0 THEN '-' ELSE '+' END"),pyspark_round(expr("abs(timezone) / 3600"))))
        df_transformed_utc = df_transformed_timezone.withColumn("timezone", concat(lit("UTC"),df_transformed_timezone.timezone ))
        
        return df_transformed_utc

    def round_values(self, silver_data):
        """This function rounds values to 2 decimal places.

        Args:
            silver_data: DataFrame to be converted.

        Returns:
            DataFrame: DataFrame with rounded values.
        """

 
        columns_to_round = ["Temperature", "Temperature_feels_like", "Minimal_Temperature", "Maximal_Temperature", "Wind_Speed"]

        silver_rounded = silver_data 

        for column in columns_to_round:
            print(f"Rounding column: {column}")
            silver_rounded = silver_rounded.withColumn(column, pyspark_round(silver_rounded[column], 2))  

        return silver_rounded

        
        

    def delete_from_city_table(self, db_name, table_name, city):
        """This function removes specific city from table  

        Args:
            db_name: database 
            table_name: table
            city: city to be deleted

        Returns:
            sql table with the city removed.
        """

        spark.sql(f"DELETE FROM {db_name}.{table_name} WHERE name = '{city}'")



    def delete_date_from_table(self, db_name, table_name, date):
        """This function removes a specific date from the table  

       Args:
            db_name: database 
            table_name: table
            date: date to be deleted

        Returns:
            sql table with the date removed.
        """

        spark.sql(f"DELETE FROM {db_name}.{table_name} WHERE current_time = '{date}'")

    def drop_table(self, db_name, table_name):

        """This function drops the table. There's also a dbutils command to remove the files too.

       Args:
            db_name: database 
            table_name: table

        Returns:
            table removed.
        """

        table_path = spark.sql(f"DESCRIBE DETAIL {db_name}.{table_name} ").collect()[0][4]

        spark.sql(f"DROP TABLE {db_name}.{table_name}")
        dbutils.fs.rm(table_path,True)


    def vacuum_table(self, db_name, table_name):

        """This function applies vacuum on the table to remove unused data. 

       Args:
            db_name: database 
            table_name: table

        Returns:
            data unused removed
        """

        spark.sql(f"VACUUM  {db_name}.{table_name} RETAIN 168 HOURS ")

    
    def optimize_table(self, db_name, table_name, key):
        """This function applies the optimized command on the table. The goal is to optimize a subsed of data. ZORDER BY was also included to replace similar data together. This helps the table to be read. 

       Args:
            db_name: database 
            table_name: table
            key: column that is used to alocate data together

        Returns:
            sql table with the date removed.
        """

        spark.sql(f"OPTIMIZE {db_name}.{table_name} ZORDER BY {key}")


# COMMAND ----------

######################################################## Main Code #######################################################

value = MercedesChallenge(base_url, api_key)

#Cities to be pulled from the API 

for city in list_of_cities:
  value.adding_city_files(city, file_path)

#Getting information from the config file 
list_of_cities_readed = value.read_dbfs_file(file_path)

#With each city, now I can pull the data from the API, transform it to spark dataframe and then store it on a bronze layer.
for city in list_of_cities_readed:
  result = value.connectToOpenWeather(city)

  result_df = value.json_to_spark(result)\
                 .withColumn("current_time", date_format(current_timestamp(), "yyyy-MM-dd"))
                  
  value.write_layer(result_df, "bronze")
  

################################################### Creating the silver layer #################################################


#Now to pull the data from the bronze layer and transform it into to a silver
silver_data = value.pull_data_from_layer("default", "bronze_weather_data")

#Only accessing the values that I'm interested 

silver_data_filtered = value.column_pruning(silver_data)

df_silver = value.convert_kelvin_to_celsius(silver_data_filtered)

df_silver = value.convert_seconds_to_timezone(df_silver)

df_silver= value.round_values(df_silver)


#Writing on silver layer
value.write_layer(df_silver, "silver")

value.vacuum_table("default", "silver_weather_data")

value.optimize_table("default", "silver_weather_data", "name")





# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.bronze_weather_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.silver_weather_data

# COMMAND ----------


