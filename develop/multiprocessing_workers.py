"""
Worker functions for multiprocessing.
"""

import json
import multiprocessing
import codecs
import sys
import logging
import time
import pymongo
import shelve

import mongodb
import utilities


def worker_parse_created_at(db_name, collection_name, batch_i, process_n, output_file):
    """
    Parse the 'created_at' field of (a batch of) tweets in MongoDB database
    
    :param db_name: the name of the MongoDB database (local) to work on
    :param collection_name: the name of the collection in the database to work on
    :param batch_i: the index of this batch, from 0 to processes number minus 1
    :param process_n: the total nubmer of processes working together
    :param output_file: the name/path of the intermediate output file this processing writes into
    """
    #logging.debug('Start')
    # initialize a new connection to MongoDB database
    collection = mongodb.initialize(db_name=db_name, collection_name=collection_name)
    
    # query the batch of tweets
    collection_size = collection.count()
    batch_size = collection_size//process_n
    skip = batch_i * batch_size
    limit = batch_size
    if batch_i == (process_n - 1): # if this is the last batch, process all left
        limit = collection_size - skip + 1
    
    time.sleep(batch_i * 0.5) # sleep few seconds to reduce the peak burden of MongoDB database
    
    cursor = collection.find(sort=[('_id', pymongo.ASCENDING)], # sort by default '_id' ascending
                             projection={'_id': 0, 'id': 1, 'created_at': 1}, # minimize I/O bandwidth
                             skip=skip,
                             limit=limit) 
    print('Process{}/{} handling documents {} to {}...'.format(batch_i, process_n, skip, skip + limit - 1))
    
    # process the 'created_at' field of each tweet and write to output file
    with codecs.open(output_file, 'w', 'utf-8') as f:
        for document in cursor:
            id_int64 = int(document['id'])
            created_at_str = document['created_at']
            created_at_timestamp = utilities.parse_tweet_created_at_str(created_at_str)
            #output_dic = {'id': id_int64, 'created_at_parsed': {'$date': created_at_timestamp_ms}}
            output_dic = {'id': id_int64, 'created_at_parsed': created_at_timestamp}
            f.write(json.dumps(output_dic) + '\n')
    #logging.debug('Done')

    
def worker_get_unique_user(db_name, collection_name, batch_i, process_n, output_file, unique_user_ids_shl, unique_user_ids_key):
    """
    Query the user object of (a batch of) unique user ids in MongoDB database
    
    :param db_name: the name of the MongoDB database (local) to work on
    :param collection_name: the name of the collection in the database to work on
    :param batch_i: the index of this batch, from 0 to processes number minus 1
    :param process_n: the total nubmer of processes working together
    :param output_file: the name/path of the intermediate output file this processing writes into
    """
    
    # initialize a new connection to MongoDB database
    collection = mongodb.initialize(db_name=db_name, collection_name=collection_name)
    
    time.sleep(batch_i * 0.5) # sleep few seconds to reduce the peak burden
    
    # read in unique user ids set from shelve
    unique_user_ids_int64_set = set()
    with shelve.open(unique_user_ids_shl, flag='r') as s:
        unique_user_ids_int64_set = s[unique_user_ids_key]
    #print('Successfully readin unique user ids set from shelve {}'.format(unique_user_ids_shl))
    
    # turn set obj into sorted list obj in order to split workload using multiprocessing
    unique_user_ids_int64_list = sorted(list(unique_user_ids_int64_set))
    
    # query the batch of users
    unique_user_ids_int64_set_size = len(unique_user_ids_int64_set)
    batch_size = unique_user_ids_int64_set_size//process_n
    skip = batch_i * batch_size
    reach = skip + batch_size
    if batch_i == (process_n - 1): # if this is the last batch, process all left
        reach = unique_user_ids_int64_set_size
        
    print('Process{}/{} querying users {} to {}...'.format(batch_i, process_n, skip, reach - 1))
    
    with codecs.open(output_file, 'w', 'utf-8') as f:
        for list_index in range(skip, reach):
            unique_user_id = unique_user_ids_int64_list[list_index]
            user_obj = collection.find_one(filter={'user.id': unique_user_id},
                                           projection={'_id': 0, 'user': 1}) # minimize I/O bandwidth
            #output_list = [(field: user_obj['user'][field]) for field in user_obj['user']]
            #output_dic = dict(output_list)
            output_dic = user_obj['user']
            f.write(json.dumps(output_dic) + '\n')
     
    print('Process{}/{} Done'.format(batch_i, process_n))

    
'''
20170422-parse_fields
Tag 'text' filed of all tweets for different topics/keywords

20170504-user_affiliation_2
Tag all tweets for keyword 'ibm' in 'text' field (multiprocessing)
'''
def worker_tag_kws_in_tw(db_name, collection_name, batch_i, process_n, output_file, kws_lst):
    """
    Tag whether a list of keywords appears in (a bath of) tweets in MongoDB database
    
    :param db_name: the name of the MongoDB database (local) to work on
    :param collection_name: the name of the collection in the database to work on
    :param batch_i: the index of this batch, from 0 to processes number minus 1
    :param process_n: the total nubmer of processes working together
    :param output_file: the name/path of the intermediate output file this processing writes into
    :param kws_lst: a list of keywords
    """
    
    '''
    Establish connection to MongoDB database and query batch of tweets
    '''
    collection = mongodb.initialize(db_name=db_name, collection_name=collection_name)
    
    collection_size = collection.count()
    batch_size = collection_size//process_n
    skip = batch_i * batch_size
    limit = batch_size
    if batch_i == (process_n - 1): # if this is the last batch, process all left
        limit = collection_size - skip + 1
    
    time.sleep(batch_i * 0.5) # sleep few seconds to reduce the peak burden of MongoDB database
    
    print('Process{}/{} handling documents {} to {}...'.format(batch_i, process_n, skip, skip + limit - 1))
    cursor = collection.find(sort=[('_id', pymongo.ASCENDING)], # sort by default '_id' ascending
                             projection={'_id': 0, 'id': 1, 'user.id': 1, 'text': 1}, # minimize I/O bandwidth
                             skip=skip,
                             limit=limit)
    
    '''
    Tag the 'text' field for each keyword in the list
    '''   
    with codecs.open(output_file, 'w', 'utf-8') as f:
        for doc in cursor:
            id_int = int(doc['id'])
            user_id_int = int(doc['user']['id'])
            text = doc['text']
            output_dict = {'id': id_int, 'user_id': user_id_int, 'text': text}
            
            for ind, kw in enumerate(kws_lst):
                res = utilities.simple_test_keyword_in_text(text=text, keyword=kw)
                output_dict['X_' + str(ind)] = res
            f.write(json.dumps(output_dict) + '\n')

"""
20170507-compare_influence_inside_outside

Filter out retweets of IBM tweets
"""

def worker_filter_rt_ibm_tweets(db_name, collection_name, batch_i, process_n, output_file, ibm_user_ids_lst):
    """
    Filter out all retweets of IBM tweets in specified collection
    
    :param db_name: the name of the MongoDB database (local) to work on
    :param collection_name: the name of the collection in the database to work on
    :param batch_i: the index of this batch, from 0 to processes number minus 1
    :param process_n: the total nubmer of processes working together
    :param output_file: the output file thie thread writting results to
    :param ibm_user_ids_lst: the list of identified IBM users' ids
    
    :return: None
    """

    '''
    Initialize a new connection to MongoDB database
    ''' 
    collection = mongodb.initialize(db_name=db_name, collection_name=collection_name)
    
    '''
    Query the batch of tweets
    '''
    collection_size = collection.count()
    batch_size = collection_size//process_n
    skip = batch_i * batch_size
    limit = batch_size
    if batch_i == (process_n - 1): # if this is the last batch, process all left
        limit = collection_size - skip + 1
    
    time.sleep(batch_i * 0.5) # sleep few seconds to reduce the peak burden of MongoDB database
    
    cursor = collection.find(sort=[('_id', pymongo.ASCENDING)], # sort by default '_id' ascending
                             projection={'_id': False},
                             skip=skip,
                             limit=limit)
    print('Process{}/{} handling documents {} to {}...'.format(batch_i, process_n, skip, skip + limit - 1))
    
    '''
    Filter retweets of IBM tweets and write to output file
    '''
    ibm_user_ids_set = set(ibm_user_ids_lst)
    with codecs.open(output_file, 'w', 'utf-8') as f:
        for doc in cursor:
            retweeted_status_user_id_int = int(doc['retweeted_status']['user']['id'])
            if retweeted_status_user_id_int in ibm_user_ids_set:
                f.write(json.dumps(doc) + '\n')
    logging.debug('Done')
