

import os
from pathlib import Path
from dotenv import load_dotenv

from app import generate_file_names
from app import logger as log
from app import metadata_pipe as m_p
from app import image_uploader

def main():

    # * initial setup 

    home = Path.home()
    load_dotenv()

    logs_folder = home / os.getenv('LOGS_FOLDER')
    log.setup_file_logging(logs_folder)

    csv_filepath = home / os.getenv('CSV_FILEPATH')
    image_folder = home / os.getenv('IMAGE_FOLDER')
    svc_acct_path = os.getenv('JSON_SERVICE_ACCT_PATH')
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = svc_acct_path

    bucket_name = os.getenv('CLOUD_STORAGE_BUCKET')
    project_id = os.getenv('PROJECT_ID')

    # * generate reference mapping based on image folder 
    reference_mapping = generate_file_names.generate_reference_mapping(image_folder, bucket_name='photo_repo_storage_bucket', prefix='images')

    if not reference_mapping:
        print('Inputs failed validation. See logs. Exiting without inserting data')
        log.logging.error('Reference Mapping Creation Failure. Check for duplicate filenames with different extensions in image folder')
        return 
    
    log.logging.info('Reference Mapping Generated')

    # * validation between image folder and supplied metadata csv 
    metadata_DF = m_p.get_csv_dataframe(csv_filepath)

    process_code, (csv_count, image_count, csv_overflow, image_overflow, match_count) = m_p.compare_images_and_csv(reference_mapping, metadata_DF)

    if process_code == 0:
        print('Inputs failed validation. See logs. Exiting without inserting data')
        log.logging.error(f'Validation Failed.\nCSV Count: {csv_count}\nImage Count: {image_count}\nCSV Overflow: {csv_overflow}\nImage Overflow: {image_overflow} Match Count: {match_count}')
        return 
    
    if process_code == 1:
        log.logging.info(f'Validation Succeeded.\nCSV Count: {csv_count}\nImage Count: {image_count}\nCSV Overflow: {csv_overflow}\nImage Overflow: {image_overflow} Match Count: {match_count}')

    # * upload images once validation succeeds 
    uploader = image_uploader.ImageUploader(bucket_name, reference_mapping, project_id)

    uploaded_files = uploader.upload_images_from_folder(image_folder)

    print(f'Uploaded {uploaded_files} image files to cloud storage.')
    log.logging.info(f'Uploaded {uploaded_files} image files to cloud storage.')

    return 


    # TODO upload metadata to Big Query (back in metadata_pipe)




    # for idx, val in enumerate(reference_mapping):
    #     print(idx, val, reference_mapping[val])


log.logging.info('Good thing happened')



# TODO main app 
# will call functions of:
#  photo_pipe
#  metadata_pipe 

# what should be located for both of them here? 
# ! google auth 


if __name__ == '__main__':
    main()