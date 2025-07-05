
from google.cloud import bigquery
import pandas as pd
import logging 


class BigQueryUploader: 
    def __init__(self, project_id, dataset_id):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = dataset_id
        self.dataset_ref = self.client.dataset(dataset_id)
        self.dataset_id = dataset_id 
        self.logger = logging.getLogger('pipeline-logger')

    def create_dataset_if_not_exists(self, location):

        try:
            dataset = self.client.get_dataset(self.dataset_ref)
            self.logger.info(f'Verified BigQuery dataset {self.dataset_id} exists')
        except Exception as e:
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = location
            dataset = self.client.create_dataset(dataset)
            self.logger.info(f'Created dataset {self.dataset_id}')

    def generate_metadata_schema(self):
        return [
        bigquery.SchemaField("filename", "STRING", mode="REQUIRED"), # integrity 
        bigquery.SchemaField("cloud_storage_url", "STRING", mode="REQUIRED"), # integrity 
        bigquery.SchemaField("upload_timestamp", "TIMESTAMP", mode="REQUIRED"), # monitoring 
        bigquery.SchemaField("file_size_bytes", "INTEGER", mode="NULLABLE"), # monitoring 
        bigquery.SchemaField("image_width", "INTEGER", mode="NULLABLE"), # info
        bigquery.SchemaField("image_height", "INTEGER", mode="NULLABLE"), # info 
        bigquery.SchemaField("description", "STRING", mode="NULLABLE"), # text description 
        bigquery.SchemaField("location", "STRING", mode="NULLABLE"), # city + state? what's the standard on this 
        bigquery.SchemaField("tags", "STRING", mode="repeated"), # thinking people or families? set up profiles / links that tags will parse to? 
        bigquery.SchemaField("start_date", "DATE", mode="NULLABLE"), # thinking specific dated photos will get an end_date of 9999-12-31
        bigquery.SchemaField("end_date", "DATE", mode="NULLABLE") # otherwise you can use the range if you don't know specific 
    ]

    def create_table_with_schema(self, table_name, schema):
        """
        Explicitly create table with defined schema
        
        Args:
            table_name: Name for the BigQuery table
            schema: List of bigquery.SchemaField objects
        """
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        table = bigquery.Table(table_id, schema=schema)
        
        try:
            table = self.client.create_table(table)
            self.logger.info(f"Created table {table_id}")
            return table
        except Exception as e:
            if "Already Exists" in str(e):
                self.logger.info(f"Table {table_id} already exists")
                return self.client.get_table(table_id)
            else:
                self.logger.error(f"Failed to create table: {str(e)}")
                raise

    
    def validate_dataframe_schema(self, df, expected_schema):
        """
        Validate DataFrame matches expected schema
        
        Args:
            df: pandas DataFrame
            expected_schema: List of bigquery.SchemaField objects
        """

        # exceptions = [
        #     "file_size_bytes",
        #     "image_width",
        #     "image_height"
        # ]

        # expected_columns = {field.name for field in expected_schema if field.name not in exceptions}
        expected_columns = {field.name for field in expected_schema}
        df_columns = set(df.columns)
        
        missing_columns = expected_columns - df_columns
        extra_columns = df_columns - expected_columns
        
        if missing_columns:
            raise ValueError(f"DataFrame missing required columns: {missing_columns}")
        
        if extra_columns:
            self.logger.warning(f"DataFrame has extra columns that will be ignored: {extra_columns}")
            # Filter DataFrame to only include expected columns
            df = df[list(expected_columns)]
        
        return df


    def upload_dataframe(self, df, table_name, schema, if_exists="append"):
        """
        Upload DataFrame with explicit schema validation and table creation
        
        Args:
            df: pandas DataFrame with your metadata
            table_name: Name for the BigQuery table
            schema: List of bigquery.SchemaField objects
            if_exists: 'replace', 'append', or 'fail'
        """
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        
        # Step 1: Validate DataFrame schema
        validated_df = self.validate_dataframe_schema(df, schema)
        
        # Step 2: Create table if it doesn't exist
        self.create_table_with_schema(table_name, schema)
        
        # Step 3: Configure the job
        job_config = bigquery.LoadJobConfig()
        job_config.schema = schema
        
        if if_exists == "replace":
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        elif if_exists == "append":
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        else:  # fail
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
        
        try:
            # Step 4: Upload the data
            job = self.client.load_table_from_dataframe(
                validated_df, table_id, job_config=job_config
            )
            
            # Wait for job to complete
            job.result()
            
            # Get the updated table
            table = self.client.get_table(table_id)
            
            self.logger.info("BigQuery Load Succeeded"
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to upload to BigQuery: {str(e)}")
            raise


def upload_metadata_to_BigQuery(project_id, dataset_id, table_name, metadata_df): 
 


    uploader = BigQueryUploader(project_id, dataset_id)

    schema = uploader.generate_metadata_schema()

    uploader.create_dataset_if_not_exists(location='US')

    metadata_df = metadata_df.copy()
    metadata_df['upload_timestamp'] = pd.Timestamp.now()


    success = uploader.upload_dataframe(
        metadata_df, 
        table_name, 
        schema, 
        if_exists='append'
    )
    
    if success:
        logging.info(f"Successfully uploaded {len(metadata_df)} records to BigQuery")
        
        # Step 6: Verify the upload
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        table = uploader.client.get_table(table_id)
        logging.info(f"Table now contains {table.num_rows} total rows")
    
    # TODO does this need to change? nest underneath if success and have an else condition? 
    # TODO if there is an error during upload_dataframe, do we skip this return? if so it's probably fine as is 
    return len(metadata_df)


    # Create the dataset 
    # call my upload function 