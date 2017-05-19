"""
Global configurations for all notebooks and files
"""

import os


# -------------------------------------------
# MongoDB configurations
# -------------------------------------------
DB_NAME = 'tweets_ek'  # database for tweets collected on expanded keywords
RAW_COL = 'c1' # collection for raw data
UPDATED_COL = 'c2'  # collection for updated data
EN_UPDATED_COL = 'c2_en' # collection for updated English tweets
NONEN_UPDATED_COL = 'c2_nonen' # collection for updated non-English tweets

# collection for parsed 'created_at' field of tweets
PARSED_CREATED_AT_COL = 'c2_parsed_created_at'

# collection for all users in 'c2' collection
USERS_COL = 'c2_users'  

# collection for affliation identification based on user's 'description' field
USER_DESC_AFFL_COL = 'c2_user_desc_affl'

# collection for 'ibm' keyword tagging results
IBM_TAG_COL = 'c2_ibm_tag'

# collection of IBM cascade tweets (retweet to IBM tweets)
# based on results of IBM affiliation identificatiton method_1
IBM_CASCADE_M1_COL = 'c2_ibm_cascade_m1'

# collection of IBM cascade tweets (retweet to IBM tweets)
# based on results of IBM affiliation identificatiton method_2
IBM_CASCADE_M2_COL = 'c2_ibm_cascade_m2'


# -------------------------------------------
# Directory configurations
# -------------------------------------------
DATA_DIR = './data'  # directory for pickled data
TMP_DIR = './tmp'  # directory for temporary data
FIG_DIR = './fig' # directory for figures

# -------------------------------------------
# Data pickle configurations
# -------------------------------------------

# list of 'id' of users identified as affiliated with IBM based on their description' field
M1_IBM_USER_IDS_PKL = os.path.join(DATA_DIR, 'm1_ibm_user_ids.lst.pkl')

# list of 'id' of users not identified as affiliated with IBM based on their description' field
M1_NONIBM_USER_IDS_PKL = os.path.join(DATA_DIR, 'm1_nonibm_user_ids.lst.pkl')

# list of 'id' of users identified as affiliated with IBM based on the proportion of their tweets including keyword 'ibm'
M2_IBM_USER_IDS_PKL = os.path.join(DATA_DIR, 'm2_ibm_user_ids.lst.pkl')

# list of 'id' of users not identified as affiliated with IBM based on the proportion of their tweets including keyword 'ibm'
M2_NONIBM_USER_IDS_PKL = os.path.join(DATA_DIR, 'm2_nonibm_user_ids.lst.pkl')

# dataframe for aggregated (different types of) tweets number and (different types of) retweets count on users
SIMPLE_INFLUENCE_PKL = os.path.join(DATA_DIR, 'simple_influence.df.pkl')

# dataframe for aggregated (observed) retweet_count of IBM tweets on IBM/non-IBM users based on method_1 affiliation.
IBM_CASCADE_M1_PKL = os.path.join(DATA_DIR, 'ibm_cascade_m1.df.pkl')

# dataframe for aggregated (observed) retweet_count of IBM tweets on IBM/non-IBM users based on method_2 affiliation.
IBM_CASCADE_M2_PKL = os.path.join(DATA_DIR, 'ibm_cascade_m2.df.pkl')
