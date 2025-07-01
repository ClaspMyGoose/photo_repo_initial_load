import logging
from datetime import datetime

# # Basic file logging setup
# def setup_file_logging(path, file_name="pipeline.log"):
#     """Set up logging to write to a file"""

#     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

#     full_path = f'{path}/{timestamp}_{file_name}'

#     logging.basicConfig(
#         level=logging.INFO,
#         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#         handlers=[
#             logging.FileHandler(full_path),
#             logging.StreamHandler()  # This also logs to console
#         ]
#     )

# * this working better 
def setup_logger(prefix, file_name='pipeline.log'):

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    logger = logging.getLogger('pipeline-logger')
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(f'{prefix}/{timestamp}_{file_name}')
    ch = logging.StreamHandler()
    fh.setLevel(logging.INFO)
    ch.setLevel(logging.INFO)

    format = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    fh.setFormatter(format)
    ch.setFormatter(format)

    logger.addHandler(ch)
    logger.addHandler(fh)

    return logger 





