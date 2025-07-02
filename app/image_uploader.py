from google.cloud import storage
import logging
from pathlib import Path

class ImageUploader:
    def __init__(self, bucket_name, ref_map, project_id=None):
        """
        Initialize the uploader with bucket name and optional project ID
        """
        self.bucket_name = bucket_name
        self.client = storage.Client(project=project_id)
        self.bucket = self.client.bucket(bucket_name)
        self.ref_map = ref_map
        self.logger = logging.getLogger('pipeline-logger')
    
    def upload_image(self, local_file_path, destination_blob_name):
        """
        Upload a single image to Cloud Storage
        
        Args:
            local_file_path: Path to the local image file
            destination_blob_name: Name for the file in Cloud Storage (optional)
        
        Returns:
            The public URL of the uploaded file
        """
        
        try:
            blob = self.bucket.blob(destination_blob_name)
            
            # Upload the file
            blob.upload_from_filename(local_file_path)
            
            self.logger.info(f"File {local_file_path} uploaded to {destination_blob_name}")
            
            # Return the public URL
            return f"gs://{self.bucket_name}/{destination_blob_name}"
            
        except Exception as e:
            self.logger.error(f"Failed to upload {local_file_path}: {str(e)}")
            raise
    
    def upload_images_from_folder(self, folder_path):
        """
        Upload all JPEG images from a folder
        
        Args:
            folder_path: Path to folder containing images
            prefix: Prefix to add to uploaded file names
        
        Returns:
            Dictionary mapping local filenames to Cloud Storage URLs
        """
        
        uploaded_file_cnt = 0
        uploaded_files = {}
        
        # ! edge case, but will only process jpg (.jpg or .jpeg) that we pre-scanned earlier via generate_file_names 
        for image_path in folder_path.iterdir():
            if image_path.is_file():
                image_name = image_path.name
                image_name_strip_ext = image_name.split('.')[0]
                ref_entry = self.ref_map.get(image_name_strip_ext)

                # ! skip files that weren't pre-scanned 
                if not ref_entry:
                    self.logger.info(f'Did not process file: {image_name}')
                    continue 
                
                try:
                    destination_name = ref_entry[2] # where we stored prefix + filename 
                    cloud_url = self.upload_image(str(image_path), destination_name)
                    uploaded_file_cnt += 1
                    uploaded_files[image_name] = cloud_url
                    
                except Exception as e:
                    self.logger.error(f"Failed to upload {image_name}: {str(e)}")
                    # Continue with other files
                    uploaded_files[image_name] = 'Failed to upload'
                    continue
        
        return (uploaded_file_cnt, uploaded_files)