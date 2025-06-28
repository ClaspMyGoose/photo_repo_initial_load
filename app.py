

import os
from dotenv import load_dotenv

load_dotenv()

svc_acct_path = os.getenv('JSON_SERVICE_ACCT_PATH')

print(svc_acct_path)
