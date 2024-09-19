#import necessary dependencies
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
from awsglue.transforms import *
from awsglue.context import GlueContext
import boto3
import io
import sys
import os
import json
import time
import requests
import pip

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize the S3 client
s3 = boto3.client('s3',region_name='eu-west-3')

# read google credentials for translate amd map apis
get_google_translate_key = s3.get_object(Bucket="package-folder", Key="googleTranslateKey.json")
content = get_google_translate_key["Body"].read().decode("utf-8")
translate_credentials_data  = json.loads(content)

def install_wheel_from_s3(bucket_name, wheel_file_key):
    local_wheel_path = f"/tmp/{os.path.basename(wheel_file_key)}"
    
    try:
        s3.download_file(bucket_name, wheel_file_key, local_wheel_path)
        pip.main(['install', local_wheel_path])
    except Exception as e:
        print(f"Error installing wheel: {e}")
    finally:
        if os.path.exists(local_wheel_path):
            os.remove(local_wheel_path)
            
#install google-translate package 
install_wheel_from_s3("package-folder", "google_cloud_translate-3.16.0-py2.py3-none-any.whl")

#import google translate and create a translate client
from google.cloud import translate_v2 as translate
translate_client = translate.Client.from_service_account_info(translate_credentials_data)

# create dynamicframes 
def create_dynamic_frame_from_csv(file):
    """
    This function creates a dynamic frame from a CSV file
  
    Parameter(csv file): The CSV file to create a dynamic frame
    
    Returns: The dynamic frame created from the CSV file
    """

    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        format_options={"withHeader": True, "separator": ","},
        connection_type="s3",
        format="csv",
        connection_options={"paths": [file], "recurse": True},
        transformation_ctx="connectGoogleAPI"
        )
    return dynamic_frame

# write to S3 buckets
def write_dynamic_frame_to_S3(df, file_name):
    dynamic_frame  = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    
    glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={"path": f"s3://airbnb-listings-data/raw-data/{file_name}"},
    format="csv"
        )



# Read the CSV files from S3 and convert to Spark DataFrames
dubai_df = create_dynamic_frame_from_csv("s3://airbnb-listings-data/raw-data/DubaiData.csv")
tokyo_df = create_dynamic_frame_from_csv('s3://airbnb_listings-bucket/raw_data/TokyoData.csv')
toronto_df = create_dynamic_frame_from_csv('s3://airbnb_listings-bucket/raw_data/TorontoData.csv')

# Convert dynamic frames to Spark DataFrames
dubaiData = dubai_df.toDF()
tokyoData = tokyo_df.toDF()
torontoData = toronto_df.toDF()

def detect_lang(row):
    try:
        result = translate_client.detect_language(row)
        return result['languages'][0]['language']
    except:
        return row

# Define the UDF for translating text
def translate_text(text):
    if isinstance(text, str):
        detected_lang = detect_lang(text)
        if detected_lang != 'en':
            try:
                translated_text = translate_client.translate(text, target_language="en", model="nmt")
                return translated_text["translatedText"]
            except Exception as e:
                print(f"Error translating text: {e}")
                return text
    return text

# Register the UDF with Spark
translate_text_udf = F.udf(translate_text, StringType())

# Translate non-English rows for each DataFrame in the dubaiData
for column in dubaiData.columns:
    if dubaiData.select(F.col(column)).filter(~F.col(column).rlike('^[a-zA-Z0-9\s\.,!?\'\"-]+$')).count() > 0:
        dubaiData = dubaiData.withColumn(column, translate_text_udf(F.col(column)))

# Translate non-English rows for each DataFrame in the tokyoData
for column in tokyoData.columns:
    if tokyoData.select(F.col(column)).filter(~F.col(column).rlike('^[a-zA-Z0-9\s\.,!?\'\"-]+$')).count() > 0:
        tokyoData = tokyoData.withColumn(column, translate_text_udf(F.col(column)))

# save to S3 bucket
write_dynamic_frame_to_S3(dubaiData, "DubaiData_translated") 

# define fetch_zipcode function
def fetch_zipcode(latitude, longitude):
    google_map_api = "your-google-map-api-key"
    try:
        url = f'https://maps.googleapis.com/maps/api/geocode/json?latlng={latitude},{longitude}&key={google_map_api}'
        response = requests.get(url)
        response.raise_for_status()  

        # Introduce time delay because google map api is max 50 requests in 1 sec
        time.sleep(0.05)  

        data = response.json()
        for result in data.get('results', []):
            for component in result.get('address_components', []):
                if 'postal_code' in component.get('types', []):
                    return component['long_name']
        return None                                         
    except requests.exceptions.RequestException as e:
        return None

# Register the UDF
fetch_zipcode_udf = F.udf(fetch_zipcode, StringType())

# apply fetch_zipcode_udf to each row in the tokyo DataFrame 
tokyoData = tokyoData.withColumn("zipcode", fetch_zipcode_udf(F.col('Latitude'), F.col('Longitude')))

# save to S3 bucket
write_dynamic_frame_to_S3(tokyoData, "TokyoData_with_zipcodes") 

# apply fetch_zipcode_udf to each row in the toronto DataFrame
torontoData = torontoData.withColumn("zipcode", fetch_zipcode_udf(F.col('Latitude'), F.col('Longitude')))

# save to S3 bucket
write_dynamic_frame_to_S3(torontoData, "TorontoData_with_zipcodes") 

sc.stop


