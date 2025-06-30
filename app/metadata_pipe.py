
# TODO read from metadata csv 
# TODO structure metadata 
# TODO upload to big query 

import pandas as pd 

def get_csv_dataframe(csv_file_path): 

    df = pd.read_csv(csv_file_path, dtype=str, sep=',', header=0)
    return df 

def compare_images_and_csv(ref_map: dict, csv_dataframe: pd.DataFrame): 
    
    csv_record_count = csv_dataframe['pic_name'].count()
    image_folder_record_count = len(ref_map)

    unique_csv_filenames = set(csv_dataframe['pic_name'])
    unique_folder_filenames = ref_map.keys()

    csv_only = unique_csv_filenames - unique_folder_filenames
    folder_only = unique_folder_filenames - unique_csv_filenames
    matched = unique_csv_filenames & unique_folder_filenames

    if csv_record_count != image_folder_record_count:
        print(f'Image Folder and CSV Record Count Mismatch')
        return (0, (csv_record_count, image_folder_record_count, len(csv_only), len(folder_only), len(matched))) 

    if len(csv_only) > 0 or len(folder_only) > 0:
        print(f'Image Folder and CSV Comparison Validation Failed')
        return (0, (csv_record_count, image_folder_record_count, len(csv_only), len(folder_only), len(matched)))  

    if len(matched) == csv_record_count:
        print(f'Validation Successful')
        return (1, (csv_record_count, image_folder_record_count, len(csv_only), len(folder_only), len(matched)))


def process_csv(ref_map, filepath):
    # this will be a call to Big Query, come back to this 

    pass 

    # TODO get csv count 
    # TODO get dict length 

    # TODO do my set operations 

    # output result and error code 