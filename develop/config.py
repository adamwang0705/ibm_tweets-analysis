"""
Global configurations for all notebooks and files
"""

import os


"""
MongoDB collections
"""
# database for tweets collected on expanded keywords
DB_NAME = 'tweets_ek-2'

# collection of raw data
TW_RAW_COL = 'tw_raw'

# colletion of native tweets
TW_NT_COL = 'tw_nt'

# collection of retweets
TW_RT_COL = 'tw_rt'

# collection for unique users information of all tweets (based on TW_RAW_COL)
USER_RAW_COL = 'user_raw'

# collection for unique users information of native tweets (based on TW_NT_COL)
USER_NT_COL = 'user_nt'

# collection for unique users information of retweets (based on TW_RT_COL)
USER_RT_COL = 'user_rt'

# collection for the bool results of whether keyword 'ibm' appears in users' 'description' field
# (based on USER_NT_COL)
USER_NT_DESC_TAG_COL = 'user_nt_desc_tag'

# collection of parsed creation time of all raw tweets
TW_RAW_PCT_COL = 'tw_raw_pct'

# collection of keywords tag on 'text' field of all raw tweets
TW_RAW_TXT_KWS_TAG_COL = 'tw_raw_txt_kws_tag'

# collection of keyword 'ibm' tag on 'text' field of all native tweets
# (based on TW_NT_COL)
TW_NT_TXT_IBM_TAG_COL = 'tw_nt_txt_ibm_tag'

# collection of retweets of IBM tweets (M1)
# based on TW_RT_COL
TW_RT_IBM_TW_COL = 'tw_rt_ibm_tw'

# ====================================================

# collection for original tweets of 'retweet tweets'
RT_ORIGIN_COL = 'c2_rt_origin'

# collection for 'ibm' keyword tagging results
IBM_TAG_COL = 'c2_ibm_tag'



"""
Directories
"""
DATA_DIR = './data'  # directory for pickled data
TMP_DIR = './tmp'  # directory for temporary data
FIG_DIR = './fig' # directory for figures


"""
Data pickles
"""

# list of 'id' field of all tweets
TW_RAW_IDS_LST_PKL = os.path.join(DATA_DIR, 'tw_raw_ids.lst.pkl')

# list of 'id' field of native tweets
TW_NT_IDS_LST_PKL = os.path.join(DATA_DIR, 'tw_nt_ids.lst.pkl')

# list of 'id' field of retweets
TW_RT_IDS_LST_PKL = os.path.join(DATA_DIR, 'tw_rt_ids.lst.pkl')


# list of 'user.id' field of all tweets
USER_RAW_IDS_LST_PKL = os.path.join(DATA_DIR, 'user_raw_ids.lst.pkl')

# list of 'user.id' field of native tweets
USER_NT_IDS_LST_PKL = os.path.join(DATA_DIR, 'user_nt_ids.lst.pkl')

# list of 'user.id' field of retweets
USER_RT_IDS_LST_PKL = os.path.join(DATA_DIR, 'user_rt_ids.lst.pkl')

# list of 'user.id' field of users of native tweets with keyword 'ibm' in their 'description' field
USER_NT_IBM_DESC_IDS_LST_PKL = os.path.join(DATA_DIR, 'user_nt_ibm_desc_ids.lst.pkl')
# list of 'user.id' field of users of native tweets without keyword 'ibm' in their 'description' field
USER_NT_NONIBM_DESC_IDS_LST_PKL = os.path.join(DATA_DIR, 'user_nt_nonibm_desc_ids.lst.pkl')

# list of 'user.id' field of users with >= 1 IBM tweet
USER_NT_IBM_TW_PROP_1_IDS_LST_PKL = os.path.join(DATA_DIR, 'user_nt_ibm_tw_prop_1_ids.lst.pkl')
# list of 'user.id' field of users with 0 IBM tweet
USER_NT_NONIBM_TW_PROP_1_IDS_LST_PKL = os.path.join(DATA_DIR, 'user_nt_nonibm_tw_prop_1_ids.lst.pkl')

# list of 'user.id' field of users with >= first quartile of IBM tweets proportion
USER_NT_IBM_TW_PROP_2_IDS_LST_PKL = os.path.join(DATA_DIR, 'user_nt_ibm_tw_prop_2_ids.lst.pkl')
# list of 'user.id' field of users with < first quartile of IBM tweets proportion
USER_NT_NONIBM_TW_PROP_2_IDS_LST_PKL = os.path.join(DATA_DIR, 'user_nt_nonibm_tw_prop_2_ids.lst.pkl')

# list of 'user.id' field of users with >= median of IBM tweets proportion
USER_NT_IBM_TW_PROP_3_IDS_LST_PKL = os.path.join(DATA_DIR, 'user_nt_ibm_tw_prop_3_ids.lst.pkl')
# list of 'user.id' field of users with < median of IBM tweets proportion
USER_NT_NONIBM_TW_PROP_3_IDS_LST_PKL = os.path.join(DATA_DIR, 'user_nt_nonibm_tw_prop_3_ids.lst.pkl')

# dataframe for aggregated (different types of) tweets number and (different types of) retweets count on users
SIMPLE_INFLUENCE_PKL = os.path.join(DATA_DIR, 'simple_influence.df.pkl')

# dataframe for aggregated retweet_count of IBM tweets on IBM/non-IBM users based on M1
IBM_CASCADE_PKL = os.path.join(DATA_DIR, 'ibm_cascade.df.pkl')

# dataframe for taged IBM follower_count of IBM users M1
IBM_FOLLOWERS_PKL = os.path.join(DATA_DIR, 'ibm_followers.df.pkl')

# dataframe for M1 IBM users' influence inside/outside IBM
# based on IBM_CASCADE_PKL and IBM_FOLLOWERS_PKL
IBM_INFLUENCE_PKL = os.path.join(DATA_DIR, 'ibm_influence.df.pkl')


"""
Misc
"""
# complete set of keywords for raw data collection
API_QUERY_KWS = ["Artificial Intelligence", "ArtificialIntelligence", "#AI", "Machine Learning", "MachineLearning",
                 "#ML", "Deep Learning" , "DeepLearning", "#DL", "OpenAI", "Neural Networks", "NeuralNetworks", "Data Mining",
                 "DataMining", "Big Data", "BigData", "Data Science", "DataScience", "Cognitive Computing", "CognitiveComputing", 
                 "Data Lake", "DataLake", "DataLakes", "AzureDataLake", "Data Warehouse", "Datawarehouse",
                 "Predictive Analytics", "PredictiveAnalytics", "#IoT", "#TensorFlow", "#RankBrain", "#DeepMind",
                 "IBM Watson", "IBMWatson", "#Watson", "BigDataAnalytics", "#Analytics", "#Azure", "Cloud Computing",
                 "CloudComputing", "#USQL", "#Hadoop", "#Spark", "#PolyBase"]
