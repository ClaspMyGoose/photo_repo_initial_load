
# TODO read from photo folder 
# TODO upload to cloud storage bucket 
 


from google.cloud import storage
import os
import pandas as pd
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ImageUploader:
    def __init__(self, bucket_name, project_id=None):
        """
        Initialize the uploader with bucket name and optional project ID
        """
        self.bucket_name = bucket_name
        self.client = storage.Client(project=project_id)
        self.bucket = self.client.bucket(bucket_name)
    
    def upload_image(self, local_file_path, destination_blob_name=None):
        """
        Upload a single image to Cloud Storage
        
        Args:
            local_file_path: Path to the local image file
            destination_blob_name: Name for the file in Cloud Storage (optional)
        
        Returns:
            The public URL of the uploaded file
        """
        if destination_blob_name is None:
            destination_blob_name = Path(local_file_path).name
        
        try:
            blob = self.bucket.blob(destination_blob_name)
            
            # Upload the file
            blob.upload_from_filename(local_file_path)
            
            logger.info(f"File {local_file_path} uploaded to {destination_blob_name}")
            
            # Return the public URL
            return f"gs://{self.bucket_name}/{destination_blob_name}"
            
        except Exception as e:
            logger.error(f"Failed to upload {local_file_path}: {str(e)}")
            raise
    
    def upload_images_from_folder(self, folder_path, prefix="images/"):
        """
        Upload all JPEG images from a folder
        
        Args:
            folder_path: Path to folder containing images
            prefix: Prefix to add to uploaded file names
        
        Returns:
            Dictionary mapping local filenames to Cloud Storage URLs
        """
        folder_path = Path(folder_path)
        uploaded_files = {}
        
        # Find all JPEG files
        jpeg_files = list(folder_path.glob("*.jpg")) + list(folder_path.glob("*.jpeg"))
        
        logger.info(f"Found {len(jpeg_files)} JPEG files to upload")
        
        for image_path in jpeg_files:
            try:
                destination_name = f"{prefix}{image_path.name}"
                cloud_url = self.upload_image(str(image_path), destination_name)
                uploaded_files[image_path.name] = cloud_url
                
            except Exception as e:
                logger.error(f"Failed to upload {image_path.name}: {str(e)}")
                # Continue with other files
                continue
        
        return uploaded_files

def process_images_and_metadata(image_folder, csv_file, bucket_name):
    """
    Main function to process images and metadata together
    
    Args:
        image_folder: Path to folder with JPEG images
        csv_file: Path to CSV file with metadata
        bucket_name: GCS bucket name
    
    Returns:
        DataFrame with metadata and Cloud Storage URLs
    """
    # Initialize uploader
    uploader = ImageUploader(bucket_name)
    
    # Upload images
    logger.info("Starting image upload process...")
    uploaded_files = uploader.upload_images_from_folder(image_folder)
    
    # Load metadata
    logger.info("Loading metadata CSV...")
    metadata_df = pd.read_csv(csv_file)
    
    # Add Cloud Storage URLs to metadata
    # Assuming the CSV has a column with image filenames (adjust column name as needed)
    if 'filename' in metadata_df.columns:
        metadata_df['cloud_storage_url'] = metadata_df['filename'].map(uploaded_files)
        
        # Flag any images that failed to upload
        missing_uploads = metadata_df['cloud_storage_url'].isna().sum()
        if missing_uploads > 0:
            logger.warning(f"{missing_uploads} images from CSV were not successfully uploaded")
    else:
        logger.error("CSV must contain a 'filename' column to match with uploaded images")
        return None
    
    return metadata_df

# Example usage
if __name__ == "__main__":
    # Configuration
    IMAGE_FOLDER = "/path/to/your/images"
    CSV_FILE = "/path/to/your/metadata.csv"
    BUCKET_NAME = "your-bucket-name"
    
    # Set up authentication (one of these methods):
    # 1. Set environment variable: GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
    # 2. Use gcloud auth: run `gcloud auth application-default login`
    # 3. On GCP services (Cloud Functions, Compute Engine, etc.), uses default service account
    
    try:
        result_df = process_images_and_metadata(IMAGE_FOLDER, CSV_FILE, BUCKET_NAME)
        
        if result_df is not None:
            logger.info(f"Successfully processed {len(result_df)} records")
            print(result_df.head())
            
            # Optionally save the enhanced metadata
            result_df.to_csv("metadata_with_urls.csv", index=False)
            
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")