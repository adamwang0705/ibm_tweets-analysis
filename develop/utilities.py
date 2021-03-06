"""
Various utility functions
"""

import datetime
import os

from config import * # import all global config variables


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


def gen_inter_filenames_list(nb_name, procedure_name, process_n, suffix):
    """
    Generate a list of intermediate output filenames for a multiprocessing procedure.
    Each process corresponds to one intermediate output file in the list identified by index.
    By default, intermediate files are written to TMP_DIR dir specified in config.py
    
    :param nb_name: name of the notebook
    :param procedure_name: name of the procedure, which is also the common name base for all intermediate output file names
    :param process_n: number of processes of this procedure
    :param suffix: common suffix for all intermediate output files of this procedure.
    :return: a list of intermediate output filenames
    """
    filenames_list = [os.path.join(TMP_DIR, '{}-{}-{}.{}'.format(nb_name, procedure_name, batch_i, suffix)) 
                      for batch_i in range(process_n)]
    return filenames_list


def simple_test_keyword_in_text(text, keyword, ignore_case=True):
    """
    Simple funtion for testing whether keyword exists in text.
    
    :param text: string to be tested on
    :param keyword: a single keyword interested in
    :param ignore_case: bool value indicates whether to ignore case for both text and keyword
    :return: bool value 
    """
    res = False
    if text and keyword:
        if ignore_case:
            res = True if keyword.lower() in text.lower() else False
        else:
            res = True if keyword in text else False
    return res
   

if __name__ == '__main__':
    test_keyword = 'ibm'
#    test_text_lst = ['List your health/beauty service/products #ibm @our http://www.healthandbeautylistings.org directory - a #trustworthy testimonial for your business.',
#                     'test onibm',
#                     'test on iloveibmhahaha',
#                     'test on IBM',
#                     'this is a test on #Ibm tag',
#                     'This is another test IBM@London',
#                     'IBMer, Mainframe supporter, Open Group Certified Architect,  Unix, Linux, #blockchain, #SAP Technologies, #Windsurfer Kitesurfer Surfer Addict  Tweets are my Own',
#                     '']
    test_text_lst = ['VP, Healthcare at IBM. Breast cancer survivor and advocate, passionate about transforming Healthcare in Canada. All thoughts and opinions are my own.',
                     'Cloud Sales @ibm_france @ibmcloud. Passionate about #digitaltransformation #cloud #AI. \nViews are my own',
                     'IBM Senior Account Manager - \nB2B Global Squad Member\n#ibm #B2B #Digitalsales',
                     'Dad. Creative (some say). Car racing enthusiast | Colombian by birth. Miamian by heart. Bostonian by fate | Global Design Director @IBM_iX - Opinions are my own']

    for ind, test_text in enumerate(test_text_lst):
        print('{}. '.format(ind + 1))
        print('"{}"'.format(test_text))
        res = simple_test_keyword_in_text(test_text, test_keyword)
        print('include keyword "{}": {}'.format(test_keyword, res))
        print('')
