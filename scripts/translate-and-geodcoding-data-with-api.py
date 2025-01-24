#import necessary dependencies
import io
import sys
import os
import json
import requests
import pip
from pyspark.context import SparkContext
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *
from awsglue.context import GlueContext
import boto3

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize the S3 client
s3_client = boto3.client('s3',region_name='eu-west-3')


# install google-translate package
local_wheel_path = f"/tmp/{os.path.basename("google_cloud_translate-3.16.0-py2.py3-none-any.whl")
s3_client.download_file(bucket_name, local_wheel_path)   # edit bucket_name
pip.main(['install', local_wheel_path])

# read google credentials for translate api
get_google_translate_key = s3.get_object(Bucket="package-folder", Key="googleTranslateKey.json")
content = get_google_translate_key["Body"].read().decode("utf-8")
translate_credentials_data  = json.loads(content)

# import google translate and create a translate client
from google.cloud import translate_v2 as translate
translate_client = translate.Client.from_service_account_info(translate_credentials_data)




# create dynamicframes 
def create_dynamic_frame_from_csv(file):
    """
    Creates a dynamic frame from a CSV file
  
    Parameter: 
        file(str):  s3_client path to the CSV file
    
    Returns: 
        DynamicFrame: DynamicFrame object created from the CSV file.
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
    """
    Writes a Spark DataFrame to an S3 bucket as a CSV file using AWS Glue.

    Parameters:
        df (pyspark.sql.DataFrame): The Spark DataFrame to be written to S3.
        file_name (str): The name of the file to be written in the S3 bucket.

    Returns:
        DynamicFrame: The AWS Glue DynamicFrame created from the input DataFrame.

    """
    dynamic_frame  = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    
    glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={"path": f"s3://airbnb-listings-data/raw-data/{file_name}"},
    format="csv"
        )
    
    return dynamic_frame
    

    
def detect_lang(row):
    """
    Detects the language of a row with the Google Translate API.

    Parameters:
        row (str): input row for which the language needs to be detected.

    Returns:
        str: The detected language code,  if detection is successful.
             Returns the input row if an exception occurs.
    """
    global translate_client

    try:
        result = translate_client.detect_language(row)
        return result['languages'][0]['language']
    except:
        return row



# Define the UDF for translating text
def translate_text(translate_client, text):
    """
    Translates non-English into English using the Google Translate API.

    Parameters:
        text (str): The input text to be translated.

    Returns:
        str: The translated text in English if translation is successful.
             Returns the original text if it is already in English or if an exception occurs.
    """

    global translate_client
        
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


# define fetch_zipcode function
def fetch_zipcode(latitude, longitude):
    """
    Fetches the postal code (zipcode) for a given latitude and longitude using the Google Maps Geocoding API.

    Parameters:
        latitude (float): The latitude of the location.
        longitude (float): The longitude of the location.

    Returns:
        str: The postal code (zipcode) if found.
             Returns None if no postal code is found or an exception occurs.
    """
    google_map_api = "your_google_map_api_key"  # edit your google map api key
    
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


def main():

    # Read the CSV files from S3 and convert to Spark DataFrames
    dubai_df = create_dynamic_frame_from_csv("s3://airbnb-listings-data/raw-data/raw_dubai_data.csv")
    tokyo_df = create_dynamic_frame_from_csv('s3://airbnb_listings-bucket/raw_data/raw_tokyo_data.csv')
    toronto_df = create_dynamic_frame_from_csv('s3://airbnb_listings-bucket/raw_data/raw_toronto_data.csv')

    # Convert dynamic frames to Spark DataFrames
    dubai_data = dubai_df.toDF()
    tokyo_data = tokyo_df.toDF()
    toronto_data = toronto_df.toDF()

    # Register the UDF with Spark
    translate_text_udf = F.udf(translate_text, StringType())
    fetch_zipcode_udf = F.udf(fetch_zipcode, StringType())


    # Translate non-English rows for each DataFrame in the dubai_data
    for column in dubai_data.columns:
        if dubai_data.select(F.col(column)).filter(~F.col(column).rlike('^[a-zA-Z0-9\s\.,!?\'\"-]+$')).count() > 0:
            dubai_data = dubai_data.withColumn(column, translate_text_udf(F.col(column)))

    # Translate non-English rows for each DataFrame in the tokyo_data
    for column in tokyo_data.columns:
        if tokyo_data.select(F.col(column)).filter(~F.col(column).rlike('^[a-zA-Z0-9\s\.,!?\'\"-]+$')).count() > 0:
            tokyo_data = tokyo_data.withColumn(column, translate_text_udf(F.col(column)))


    # save dubai_data to S3 bucket
    write_dynamic_frame_to_S3(dubai_data, "dubai_data_translated") 


    # apply fetch_zipcode_udf to each row in the tokyo and toronto DataFrames 
    tokyo_data = tokyo_data.withColumn("zipcode", fetch_zipcode_udf(F.col('Latitude'), F.col('Longitude')))
    toronto_data = toronto_data.withColumn("zipcode", fetch_zipcode_udf(F.col('Latitude'), F.col('Longitude')))

    # save to S3 bucket
    write_dynamic_frame_to_S3(tokyo_data, "tokyo_data_with_zipcodes") 
    write_dynamic_frame_to_S3(toronto_data, "toronto_data_with_zipcodes") 

    sc.stop

if __name__ == "__main__":
    main()


