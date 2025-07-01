

import os
from pathlib import Path
from dotenv import load_dotenv
import logging
import pandas as pd 
from app import generate_file_names
from app import logger as log
from app import metadata_transform as m_t
from app import image_uploader
from app import metadata_uploader as m_u

def main():

    # * initial setup 

    home = Path.home()
    load_dotenv()

    logs_folder = home / os.getenv('LOGS_FOLDER')
    #log.setup_file_logging(logs_folder)
    log.setup_logger('logs')
    logger = logging.getLogger('pipeline-logger')

    csv_filepath = home / os.getenv('CSV_FILEPATH')
    image_folder = home / os.getenv('IMAGE_FOLDER')
    svc_acct_path = os.getenv('JSON_SERVICE_ACCT_PATH')
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = svc_acct_path

    bucket_name = os.getenv('CLOUD_STORAGE_BUCKET')
    project_id = os.getenv('PROJECT_ID')
    dataset_id = 'photo_repo'
    table_name = 'photo_metadata'

    # * generate reference mapping based on image folder 
    reference_mapping = generate_file_names.generate_reference_mapping(image_folder, bucket_name='photo_repo_storage_bucket', prefix='images')

    if not reference_mapping:
        print('Inputs failed validation. See logs. Exiting without inserting data')
        logger.error('Reference Mapping Creation Failure. Check for duplicate filenames with different extensions in image folder')
        return 
    
    logger.info('Reference Mapping Generated')

    # * validation between image folder and supplied metadata csv 
    metadata_DF = m_t.get_csv_dataframe(csv_filepath)

    process_code, (csv_count, image_count, csv_overflow, image_overflow, match_count) = m_t.compare_images_and_csv(reference_mapping, metadata_DF)

    if process_code == 0:
        print('Inputs failed validation. See logs. Exiting without inserting data')
        logger.error(f'Validation Failed.\nCSV Count: {csv_count}\nImage Count: {image_count}\nCSV Overflow: {csv_overflow}\nImage Overflow: {image_overflow} Match Count: {match_count}')
        return 
    
    if process_code == 1:
        logger.info(f'Validation Succeeded. CSV Overflow: {csv_overflow} -- Image Overflow: {image_overflow} -- Match Count: {match_count}')

    # * upload images once validation succeeds 
    uploader = image_uploader.ImageUploader(bucket_name, reference_mapping, project_id)

    uploaded_cnt, uploaded_files = uploader.upload_images_from_folder(image_folder)

    print(f'Uploaded {uploaded_cnt} image files to cloud storage.')
    logger.info(f'Uploaded {uploaded_cnt} image files to cloud storage.')

    # * transform metadata_DF in preparation for upload 

    transformed_df = m_t.transform_dataframe(reference_mapping, metadata_DF)

    # * upload metadata to bigquery (includes dataset and table creation)

    

    try:
        records_inserted = m_u.upload_metadata_to_BigQuery(project_id, dataset_id, table_name, transformed_df)
        logger.info(f'Successfully inserted {records_inserted} records to BigQuery')
        if records_inserted == uploaded_cnt:
            logger.info(f'Records Inserted: {records_inserted} and Images Uploaded: {uploaded_cnt} match.')
        else:
            logger.error(f'Records Inserted: {records_inserted} and Images Uploaded: {uploaded_cnt} don\'t match :(.')
    except Exception as e:
        logger.error('Big Query Upload Failed')
        print('Big Query Upload Failed')


    logger.info('Pipeline execution finished')
    print('Pipeline execution finished')


if __name__ == '__main__':
    main()