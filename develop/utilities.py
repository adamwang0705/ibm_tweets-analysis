"""
Various utility functions
"""

import datetime
import os


def parse_tweet_created_at_str(created_at_str):
    """
    Parse fixed format string of 'created_at' field returned by Twitter into Unix timestamp
    Example fixed format string of 'created_at' field: "Wed Aug 27 13:08:45 +0000 2008"
    See https://pymotw.com/3/datetime/index.html
    
    :param created_at_str: the string of 'created_at' field
    :return: Unix timestamp
    """
    tweet_created_at_str_template = '%a %b %d %H:%M:%S %z %Y'
    datetime_obj = datetime.datetime.strptime(created_at_str, tweet_created_at_str_template)
    timestamp = datetime_obj.timestamp()
    #timestamp_ms = int(timestamp)*1000
    return timestamp


def gen_inter_filenames_list(procedure_name, process_n, suffix):
    """
    Generate a list of intermediate output filenames for a multiprocessing procedure.
    Each process corresponds to one intermediate output file in the list identified by index.
    
    :param procedure_name: the name of the procedure, which is also the common name base for all intermediate output file names
    :param process_n: number of processes of this procedure
    :param suffix: common suffix for all intermediate output files of this procedure.
    :return: a list of intermediate output filenames
    """
    filenames_list = [os.path.join('inter', '{}-{}.{}'.format(procedure_name, batch_i, suffix)) 
                      for batch_i in range(process_n)]
    return filenames_list
