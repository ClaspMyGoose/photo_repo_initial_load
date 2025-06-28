

import os
from pathlib import Path
from dotenv import load_dotenv

from app import generate_file_names

def main():

    home = Path.home()
    load_dotenv()

    image_folder = home / os.getenv('IMAGE_FOLDER')
    svc_acct_path = os.getenv('JSON_SERVICE_ACCT_PATH')

    reference_mapping = generate_file_names.generate_reference_mapping(image_folder, "test")

    for idx, val in enumerate(reference_mapping):
        print(idx, val, reference_mapping[val])




# TODO main app 
# will call functions of:
#  photo_pipe
#  metadata_pipe 

# what should be located for both of them here? 
# ! google auth 


if __name__ == '__main__':
    main()