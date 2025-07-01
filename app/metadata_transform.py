
# TODO read from metadata csv 
# TODO structure metadata 
# TODO upload to big query 

import pandas as pd 

def get_csv_dataframe(csv_file_path): 

    df = pd.read_csv(
        csv_file_path, 
        # dtype={
        #     'pic_name': str,
        #     'description': str,
        #     'location': str,
        #     'tags': str,
        #     'start_date': 'int64',
        #     'end_date': 'int64'

        # },  
        dtype=str,
        #parse_dates=['start_date', 'end_date'],
        #date_format='%Y/%m/%d',
        sep=',', 
        header=0
    )
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


def transform_dataframe(ref_map, df):
   
    df = df.copy()
    df['filename'] = df['pic_name'].map(lambda x: ref_map.get(x, 'none')[2][7:])
    df['cloud_storage_url'] = df['pic_name'].map(lambda x: ref_map.get(x, 'none')[1])
    df['tags'] = df['tags'].str.split(',').apply(lambda x: [item.strip() for item in x])
    df['image_width'] = None
    df['image_height'] = None
    df['file_size_bytes'] = None
    df['start_date'] = pd.to_datetime(df['start_date'], format='%Y/%m/%d', errors='raise')
    df['end_date'] =  pd.to_datetime(df['end_date'], format='%Y/%m/%d', errors='raise')



    return df