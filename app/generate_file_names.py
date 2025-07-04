

from datetime import datetime
from pathlib import Path

def generate_unique_blob_name(filename: str, prefix: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}/{timestamp}_{filename}"

def generate_cloud_uri(bucket_name: str, blob_name: str) -> str:
    """Generate predictable Cloud Storage URI"""
    return f"gs://{bucket_name}/{blob_name}"

def strip_extension(filename: str) -> str:
    return filename.split('.')[0]

def generate_reference_mapping(src_folder: str, bucket_name: str, prefix: str) -> dict[str: list]:

    """Scans folder of jpg images and generates unique names and GCP cloud URIs"""
    # * purpose is to ensure we maintain referential integrity between pictures we upload and the metadata for said pictures 

    src_folder = Path(src_folder)
    
    # dict of arrays
    # key = file name as we received it, minus extension
    # value = array of helpful IDs 
    # [0] = file name as we received it (includes extension)
    # [1] = complete cloud URI / public URL we generate -> # * current location on GCP 
    # [2] = unique prefix + filename we generate with timestamp to avoid namespace issues -> # * unique name for the resource, irrespective of Google 
    reference_mapping = {}
     
    jpeg_files = list(src_folder.glob("*.jpg")) + list(src_folder.glob("*.jpeg"))

    for image in jpeg_files:
        image_name = Path(image).name 
        stripped_filename = strip_extension(image_name)
        unique_blob_name = generate_unique_blob_name(image_name, prefix)
        cloud_uri = generate_cloud_uri(bucket_name, unique_blob_name)
        dict_ref = reference_mapping.get(stripped_filename)
        if dict_ref:
            print('Check image folder for duplicate filenames with different extensions - i.e. Photo001.jpg and Photo001.jpeg')
            return {}
        reference_mapping[stripped_filename] = [image_name, cloud_uri, unique_blob_name]
        
    return reference_mapping