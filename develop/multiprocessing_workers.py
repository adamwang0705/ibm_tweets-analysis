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

    
def worker_tag_keyword_in_tweet(db_name, collection_name, batch_i, process_n, output_file, keyword):
    """
    Tag whether the keyword appears in (a bath of) tweets in MongoDB database
    
    :param db_name: the name of the MongoDB database (local) to work on
    :param collection_name: the name of the collection in the database to work on
    :param batch_i: the index of this batch, from 0 to processes number minus 1
    :param process_n: the total nubmer of processes working together
    :param output_file: the name/path of the intermediate output file this processing writes into
    """

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
                             projection={'_id': 0, 'id': 1, 'user.id': 1, 'text': 1}, # minimize I/O bandwidth
                             skip=skip,
                             limit=limit)
    print('Process{}/{} handling documents {} to {}...'.format(batch_i, process_n, skip, skip + limit - 1))
    
    # tag the 'text' field of each tweet whether it contains keyword and write to output file
    with codecs.open(output_file, 'w', 'utf-8') as f:
        for document in cursor:
            id_int64 = int(document['id'])
            user_id_int64 = int(document['user']['id'])
            text = document['text']
            text_ibm = utilities.simple_test_keyword_in_text(text=text, keyword=keyword)
            
            output_dic = {'id': id_int64, 'user_id': user_id_int64, 'text_ibm': text_ibm, 'text': text}
            f.write(json.dumps(output_dic) + '\n')
    logging.debug('Done')

def worker_filter_ibm_cascade_tweets(db_name, collection_name, batch_i, process_n, output_file, ibm_user_ids_set):
    """
    Filter out all retweet/quote/reply tweets to IBM user/tweet in MondoDB database
    
    :param db_name: the name of the MongoDB database (local) to work on
    :param collection_name: the name of the collection in the database to work on
    :param batch_i: the index of this batch, from 0 to processes number minus 1
    :param process_n: the total nubmer of processes working together
    :param output_file: the output file thie thread writting results to
    :param ibm_user_ids_lst: the list of identified IBM users' ids
    
    :param output_file: the name/path of the intermediate output file this processing writes into
    """

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
                             projection={'_id': False},
                             skip=skip,
                             limit=limit)
    print('Process{}/{} handling documents {} to {}...'.format(batch_i, process_n, skip, skip + limit - 1))
    
    # filter tweets and write to output file
    with codecs.open(output_file, 'w', 'utf-8') as f:
        for doc in cursor:
            flag = False
            doc_dict = dict(doc)
            # if this tweet is a retweet of IBM tweet
            if doc_dict.get('retweeted_status'):                
                retweeted_status_user_id_int = int(doc['retweeted_status']['user']['id'])
                if retweeted_status_user_id_int in ibm_user_ids_set:
                    flag = True
            ## if this tweet is a quote of IBM tweet
            #if doc_dict.get('quoted_status'):
            #    quoted_status_user_id_int = int(doc['quoted_status']['user']['id'])
            #    if quoted_status_user_id_int in ibm_user_ids_set:
            #        flag = True
            ## if this tweet is a reply to a IBM tweet
            #if doc_dict.get('in_reply_to_user_id'):
            #    if int(doc['in_reply_to_user_id']) in ibm_user_ids_set:
            #        flag = True
            if flag:
                f.write(json.dumps(doc) + '\n')
    logging.debug('Done')