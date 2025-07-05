
# TODO imports 

import pytest
import os
from dotenv import load_dotenv
from pathlib import Path
# ! works when calling pytest from cmd line 
from . import generate_file_names
from . import metadata_transform
# ! works when running via VS Code or normal python invocation 
# import generate_file_names
# import metadata_transform


class TestPipeline:

    def test_good_generate_file_names_generate_reference_mapping(self):
        
        # generate_reference_mapping(src_folder: str, bucket_name: str, prefix: str)
        # when given good input, we get a dict back with expected keys, each k/v pair containing an array of length 3 
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


   
   
    def test_good_metadata_transform_compare_images_and_csv(self):
        # with good input, returns the object in the shape we're expecting 
        home = Path.home() 
        load_dotenv()
        good_image_folder = home / os.getenv('IMAGE_FOLDER')
        good_csv_file = home / os.getenv('CSV_FILEPATH')

        reference_mapping = generate_file_names.generate_reference_mapping(good_image_folder, load_dotenv('CLOUD_STORAGE_BUCKET'), prefix='images')
        csv_df = metadata_transform.get_csv_dataframe(good_csv_file)


        result_tuple = metadata_transform.compare_images_and_csv(reference_mapping, csv_df)
        assert result_tuple

        result_code, (csv_records, image_records, csv_overflow, image_overflow, matched_cnt) = result_tuple
        assert result_code == 1 
        assert csv_records == image_records
        assert csv_records == matched_cnt
        assert csv_overflow == 0
        assert image_overflow == 0 
        

    #     # * metadata_transform 
    def test_good_metadata_transform_transform_dataframe(self):
    # with good input, return the dataframe with the additional cols we're expecting    
        home = Path.home() 
        load_dotenv()
        good_image_folder = home / os.getenv('IMAGE_FOLDER')
        good_csv_file = home / os.getenv('CSV_FILEPATH')


        transform_csv = metadata_transform.get_csv_dataframe(good_csv_file)

        csv_cols = transform_csv.columns
        expected_cols = ['pic_name','description','location','tags','start_date','end_date']

        for name in expected_cols:
            assert name in csv_cols
        


    def test_bad_generate_file_names_generate_reference_mapping(self):
    # when given bad input (duplicate file names, diff extensions), we get an empty dict back 

        home = Path.home() 
        load_dotenv()
        bad_image_folder = home / os.getenv('BAD_IMAGE_FOLDER')

        
        reference_mapping = generate_file_names.generate_reference_mapping(bad_image_folder, load_dotenv('CLOUD_STORAGE_BUCKET'), prefix='images')
        
        assert isinstance(reference_mapping, dict)
        assert len(reference_mapping) == 0
        assert not reference_mapping


    
    
    def test_bad_metadata_transform_compare_images_and_csv(self):
        
        home = Path.home() 
        load_dotenv()
        good_image_folder = home / os.getenv('IMAGE_FOLDER')
        good_csv_file = home / os.getenv('CSV_FILEPATH')
        overflow_image_folder = home / os.getenv('OVERFLOW_IMAGE_FOLDER')
        mismatch_csv = home / os.getenv('MISMATCH_CSV_FILEPATH')

        overflow_ref_map = generate_file_names.generate_reference_mapping(overflow_image_folder, load_dotenv('CLOUD_STORAGE_BUCKET'), prefix='images')
        csv_df = metadata_transform.get_csv_dataframe(good_csv_file)
        good_ref_map = generate_file_names.generate_reference_mapping(good_image_folder, load_dotenv('CLOUD_STORAGE_BUCKET'), prefix='images')
        mismatch_csv_df = metadata_transform.get_csv_dataframe(mismatch_csv)

        # scenario 1 ref_map overflows csv 
            # have an image folder with additional rows 
        scenario_1 = metadata_transform.compare_images_and_csv(overflow_ref_map, csv_df)

        assert scenario_1
        result_code, (csv_records, image_records, csv_overflow, image_overflow, matched_cnt) = scenario_1

        assert result_code == 0 
        assert csv_records < image_records
        assert image_overflow == 1 



        # scenario 2 csv overflows ref_map 
            # change csv_df to include an additional row 

        bad_df = csv_df.copy()
        bad_df.loc[len(csv_df)] = ['5','mom with two pairs of glasses on','Nashville','mom','2025/2/4','null']

        scenario_2 = metadata_transform.compare_images_and_csv(good_ref_map, bad_df)
        
        assert scenario_2
        result_code, (csv_records, image_records, csv_overflow, image_overflow, matched_cnt) = scenario_2

        assert result_code == 0 
        assert csv_records > image_records
        assert csv_overflow == 1



        # scenario 3 counts match but different set member 
            # use my bad_sample_photos and a good image folder 

        scenario_3 =  metadata_transform.compare_images_and_csv(overflow_ref_map, mismatch_csv_df)

        assert scenario_3
        result_code, (csv_records, image_records, csv_overflow, image_overflow, matched_cnt) = scenario_3

        assert result_code == 0 
        assert csv_records == image_records
        assert matched_cnt == 3 
        assert image_overflow == 1
        assert csv_overflow == 1

       


if __name__ == "__main__":
    # Run tests with: python test_pipeline.py
    pytest.main([__file__])


