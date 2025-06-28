

from datetime import datetime
from pathlib import Path

def generate_unique_blob_name(filename, prefix="images"):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}/{timestamp}_{filename}"

def generate_cloud_uri(bucket_name, blob_name):
    """Generate predictable Cloud Storage URI"""
    return f"gs://{bucket_name}/{blob_name}"

def generate_reference_mapping(src_folder, bucket_name):

    """Scans folder of jpg images and generates unique names and GCP cloud URIs"""
    # * purpose is to ensure we maintain referential integrity between pictures we upload and the metadata for said pictures 

    src_folder = Path(src_folder)
    
    # dict of arrays
    # key = file name as we received it 
    # value = array of helpful IDs 
    # [0] = complete cloud URI / public URL we generate -> # * current location on GCP 
    # [1] = unique filename we generate with timestamp to avoid namespace issues -> # * unique name for the resource, irrespective of Google 
    reference_mapping = {}
     
    jpeg_files = list(src_folder.glob("*.jpg")) + list(src_folder.glob("*.jpeg"))

    for image in jpeg_files:
        image_name = Path(image).name 
        unique_blob_name = generate_unique_blob_name(image_name)
        cloud_uri = generate_cloud_uri(bucket_name, unique_blob_name)
        reference_mapping[image_name] = [cloud_uri, unique_blob_name]


    return reference_mapping