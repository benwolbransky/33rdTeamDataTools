import os
from datetime import date

STR_TODAY = date.today().strftime('%Y_%m_%d')
DEFAULT_SEASON = 2023 # can be updated if needed 

#Authentication with APIs
EMAIL = "email@domain.com" #Your Email for TruMedia login
TOKEN = "token" # Your Access Token for TruMedia

# file location
BASE_DIR = os.getcwd()
