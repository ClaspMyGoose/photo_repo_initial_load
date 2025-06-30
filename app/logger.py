import logging
from datetime import datetime

# Basic file logging setup
def setup_file_logging(path, file_name="pipeline.log"):
    """Set up logging to write to a file"""

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # * Could be useful, feed this to FileHandler instead  
    # full_path = f'{path}/{timestamp}_{file_name}'

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(file_name),
            logging.StreamHandler()  # This also logs to console
        ]
    )

    




