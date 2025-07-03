
# TODO imports 

import pytest
import pandas as pd
import tempfile
import os
from dotenv import load_dotenv
from pathlib import Path
from unittest.mock import patch, MagicMock
from . import generate_file_names

# TODO define a couple tests 
# class TestDataValidation:
#     """Test data validation functions"""
    
#     def test_csv_filename_validation(self):
#         """Test that CSV contains required filename column"""
#         # Create test CSV
#         test_data = pd.DataFrame({
#             'filename': ['photo1.jpg', 'photo2.jpg'],
#             'description': ['Beach', 'Mountain']
#         })
        
#         # Test that filename column exists
#         assert 'filename' in test_data.columns
#         assert len(test_data) == 2
#         assert test_data['filename'].dtype == 'object'
    
#     def test_filename_matching(self):
#         """Test filename matching between CSV and folder"""
#         csv_files = {'photo1.jpg', 'photo2.jpg', 'photo3.jpg'}
#         folder_files = {'photo1.jpg', 'photo2.jpg', 'photo4.jpg'}
        
#         # Test set operations
#         matched = csv_files & folder_files
#         csv_only = csv_files - folder_files
#         folder_only = folder_files - csv_files
        
#         assert matched == {'photo1.jpg', 'photo2.jpg'}
#         assert csv_only == {'photo3.jpg'}
#         assert folder_only == {'photo4.jpg'}
    
#     def test_date_parsing(self):
#         """Test date parsing works correctly"""
#         test_dates = ['2025/1/1', '2025/2/4', '2025/12/31']
#         parsed_dates = pd.to_datetime(test_dates, format='%Y/%m/%d')
        
#         assert len(parsed_dates) == 3
#         assert parsed_dates.dtype == 'datetime64[ns]'
#         assert parsed_dates[0].year == 2025

# class TestFileOperations:
#     """Test file system operations"""
    
#     def test_image_file_discovery(self):
#         """Test finding JPEG files in a folder"""
#         with tempfile.TemporaryDirectory() as temp_dir:
#             temp_path = Path(temp_dir)
            
#             # Create test files
#             (temp_path / 'photo1.jpg').touch()
#             (temp_path / 'photo2.jpeg').touch()
#             (temp_path / 'document.txt').touch()
            
#             # Test glob pattern
#             jpg_files = list(temp_path.glob('*.jpg'))
#             jpeg_files = list(temp_path.glob('*.jpeg'))
            
#             assert len(jpg_files) == 1
#             assert len(jpeg_files) == 1
#             assert jpg_files[0].name == 'photo1.jpg'
    
#     def test_cloud_uri_generation(self):
#         """Test Cloud Storage URI generation"""
#         bucket_name = "test-bucket"
#         filename = "photo1.jpg"
#         prefix = "images/"
        
#         expected_uri = f"gs://{bucket_name}/{prefix}{filename}"
#         actual_uri = f"gs://{bucket_name}/{prefix}{filename}"
        
#         assert actual_uri == expected_uri

# class TestDataFrameOperations:
#     """Test pandas DataFrame operations"""
    
#     def test_dictionary_mapping(self):
#         """Test mapping dictionary values to DataFrame columns"""
#         df = pd.DataFrame({'pic_name': ['IMG_001.jpg', 'IMG_002.jpg']})
#         ref_map = {
#             'IMG_001.jpg': 'sunset.jpg',
#             'IMG_002.jpg': 'mountain.jpg'
#         }
        
#         df['filename'] = df['pic_name'].map(ref_map)
        
#         assert df['filename'].iloc[0] == 'sunset.jpg'
#         assert df['filename'].iloc[1] == 'mountain.jpg'
    
#     def test_array_column_creation(self):
#         """Test creating array columns for BigQuery"""
#         df = pd.DataFrame({
#             'filename': ['photo1.jpg', 'photo2.jpg'],
#             'tags_string': ['nature,sunset', 'mountain,landscape']
#         })
        
#         df['tags_array'] = df['tags_string'].str.split(',')
        
#         assert isinstance(df['tags_array'].iloc[0], list)
#         assert df['tags_array'].iloc[0] == ['nature', 'sunset']
#         assert len(df['tags_array'].iloc[1]) == 2

# class TestBigQueryIntegration:
#     """Test BigQuery operations (mocked)"""
    
#     @patch('google.cloud.bigquery.Client')
#     def test_schema_validation(self, mock_client):
#         """Test BigQuery schema creation"""
#         from google.cloud import bigquery
        
#         # Define test schema
#         schema = [
#             bigquery.SchemaField("filename", "STRING", mode="REQUIRED"),
#             bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
#         ]
        
#         # Test schema structure
#         assert len(schema) == 2
#         assert schema[0].name == "filename"
#         assert schema[0].field_type == "STRING"
#         assert schema[1].mode == "REPEATED"
    
#     def test_dataframe_bigquery_compatibility(self):
#         """Test DataFrame structure for BigQuery upload"""
#         df = pd.DataFrame({
#             'filename': ['photo1.jpg', 'photo2.jpg'],
#             'tags': [['nature', 'sunset'], ['mountain', 'landscape']],
#             'upload_timestamp': pd.Timestamp.now()
#         })
        
#         # Test required columns exist
#         assert 'filename' in df.columns
#         assert 'tags' in df.columns
        
#         # Test data types
#         assert isinstance(df['tags'].iloc[0], list)

# class TestErrorHandling:
#     """Test error handling scenarios"""
    
#     def test_missing_files_handling(self):
#         """Test handling of missing image files"""
#         csv_files = {'photo1.jpg', 'photo2.jpg', 'photo3.jpg'}
#         folder_files = {'photo1.jpg', 'photo2.jpg'}  # Missing photo3.jpg
        
#         missing_files = csv_files - folder_files
        
#         assert len(missing_files) == 1
#         assert 'photo3.jpg' in missing_files
    
#     def test_invalid_date_handling(self):
#         """Test handling of invalid dates"""
#         test_dates = ['2025/1/1', 'invalid_date', '2025/12/31']
#         parsed_dates = pd.to_datetime(test_dates, errors='coerce')
        
#         assert pd.notna(parsed_dates[0])  # Valid date
#         assert pd.isna(parsed_dates[1])   # Invalid date becomes NaT
#         assert pd.notna(parsed_dates[2])  # Valid date

# TODO actual testing here. 
class TestPipeline:
    """Test the complete pipeline flow"""


    # def test_pipeline_with_sample_data(self):
    #     """Test complete pipeline with sample data"""
    #     # Create sample DataFrame
    #     df = pd.DataFrame({
    #         'filename': ['photo1.jpeg', 'photo2.jpg'],
    #         'upload_timestamp': ['photo1.jpeg', 'photo2.jpg'],
    #         'description': ['Test photo 1', 'Test photo 2'],
    #         'location': ['photo1.jpeg', 'photo2.jpg'],
    #         'tags': ['photo1.jpeg', 'photo2.jpg'],
    #         'start_date': ['2025/1/1', '2025/1/2'],
    #         'end_date': ['2025/1/1', '2025/1/2'],
    #     })

    #     df['image_width'] = None
    #     df['image_height'] = None
    #     df['file_size_bytes'] = None
    #     df['upload_timestamp'] = pd.Timestamp.now()


        


    #     # Add cloud storage URLs
    #     df['cloud_storage_url'] = df['filename'].apply(
    #         lambda x: f"gs://test-bucket/images/{x}"
    #     )
        
    #     # Add timestamp
    #     df['upload_timestamp'] = pd.Timestamp.now()
        
    #     # Validate final structure
    #     assert len(df) == 2
    #     assert 'cloud_storage_url' in df.columns
    #     assert 'upload_timestamp' in df.columns
    #     assert df['cloud_storage_url'].str.startswith('gs://').all()


    # TODO this is working but we should split it up into 1 fx() per test 
    def test_outputs_with_good_data(self):
        # ! my sample data works here 
        # * generate file names
        home = Path.home() 
        load_dotenv()
        good_image_folder = home / os.getenv('IMAGE_FOLDER')

        
        reference_mapping = generate_file_names.generate_reference_mapping(good_image_folder, load_dotenv('CLOUD_STORAGE_BUCKET'), prefix='images')
        
        validation_arr = []
        for image in Path.iterdir(good_image_folder):
            strip_ext = ''
            if image.is_file():
                print(image.name)
                match image.name[-4:]:
                    case '.jpg':
                        strip_ext = image.name[:-4]
                    case 'jpeg':
                        strip_ext = image.name[:-5]
                    case _:
                        continue 
                validation_arr.append(strip_ext)
            else:
                continue 

        returned_items = reference_mapping.keys()        
        assert len(reference_mapping) == len(validation_arr)

        for image_name in validation_arr:
            assert image_name in returned_items
            assert isinstance(reference_mapping.get(image_name),list)
            assert len(reference_mapping.get(image_name)) == 3


        # generate_reference_mapping(src_folder: str, bucket_name: str, prefix: str)
            # TODO when given good input, we get a dict back with expected keys, each k/v pair containing an array of length 3 
        # * image uploader 
        # upload_image - # TODO need to use patch and magicMock here 
            # TODO with good input returns a string that matches our pattern 
        # upload_images_from_folder - # TODO need to use patch and magicMock here 
            # TODO with good input, returns a count and a dict. count should match dict items whose value <> 'Failed to upload'
            # TODO with good input, our count minus "Failed to upload" should equal length of our reference mapping 
        # * metadata_transform 
        # compare_images_and_csv
        # TODO with good input, returns the object in the shape we're expecting 
            # this one is simple 
        # transform_dataframe
        # TODO with good input, return the dataframe with the additional cols we're expecting 
        # * metadata_uploader 
        # upload_dataframe - # TODO need to use patch and magicMock here 
            # TODO with good input and connection, returns True 
        # upload_metadata_to_BigQuery - # TODO need to use patch and magicMock her
            # TODO with good input, return the number of rows inserted 
        # * app 
        # main 

    def test_outputs_with_bad_data(self):
        pass 
        # TODO need to copy sample photos, fuck up the csv, maybe remove some photos 
        # * generate file names 
        # generate_reference_mapping
             # TODO when given bad input (duplicate file names, diff extensions), we get an empty dict back 
        # * image uploader 
        # upload_image # TODO need to use patch and magicMock here 
            # TODO with bad input, raises error 
        # upload_images_from_folder # TODO need to use patch and magicMock here 
            # TODO with bad input, returns a count and a dict. count should match dict items whose value <> 'Failed to upload' (however many good items)
            # TODO with bad input, our count minus "Failed to upload" should equal the # of our good items 
        # * metadata_transform 
        # compare_images_and_csv
            # TODO with bad input, returns the object in the error shape we're expecting 
            # we have a couple bad scenarios, flesh out each one 
        # * metadata_uploader 
        # upload_dataframe - # TODO need to use patch and magicMock here 
            # TODO with bad input or connection, raises error 
        # upload_metadata_to_BigQuery - # TODO need to use patch and magicMock her
            # TODO with bad input or connection, make sure we are catching the error and skipping the return 
            # TODO other wise need to modify code slightly 
        # * app 
        # main 

if __name__ == "__main__":
    # Run tests with: python test_pipeline.py
    pytest.main([__file__])
# TODO bash script to run tests 

