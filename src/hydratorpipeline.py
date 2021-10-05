#
#  functions to identify external interactions for a given dataset
# 
#
#   Steps:
#   1. read a single file in a folder containing tweets 
#   2. extract external interactions (retweet, reply, quoute). If the userID is absent, it refers to an 
#      external user (possibly not banned yet or msg still available)
#   3. write out list of tweetids to download
#   4. download the tweetids using adequate tool
#   5. store raw data on separate disk for later use
#
#
# NOTES:
# -----
#
# Some downloaded files generate errors upon parsing them due to the presence of a NUL in a line. We were able to fix this by using:
#       sed -i 's/\x0//g' path/to/file.csv
# afterwards the generation of the external files went well and extra data was succesfully downloaded and added to the repository
#

import csv
import os
import tqdm # for progress bar
import json
from twarc import Twarc 
from credentials import CONSUMER_KEY, CONSUMER_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET


def externaltweets(path, outpath_rt=None, outpath_rp=None, outpath_qt=None):
    """generate list of external interactions

    list is stored in same folder, for a specific filename with the _interactiontype annotation
    """
    # output for lists
    outfolder = os.path.split(path)[0]
    fname = os.path.split(path)[1]
    ind = fname.find('tweet')
    outname = fname
    if outpath_qt is None:
        outpath_qt = os.path.join(outfolder, '{}{}'.format(outname, '_quotes'))
    if outpath_rt is None:
        outpath_rt = os.path.join(outfolder, '{}{}'.format(outname, '_retweets'))
    if outpath_rp is None:
        outpath_rp = os.path.join(outfolder, '{}{}'.format(outname, '_replies'))

    # processing part
    retweets = set()
    replies = set()
    quotes = set()

    with open(path, newline='') as csvfile:  
        reader = csv.DictReader(csvfile)
        for row in reader:
            # external retweets
            if (len(row['retweet_userid']) == 0) & (len(row['retweet_tweetid']) > 0):
                retweets.add(row['retweet_tweetid'])

            # external replies
            if (len(row['in_reply_to_userid']) >= 0) & (len(row['in_reply_to_tweetid']) > 0):
                replies.add(row['in_reply_to_tweetid'])
            
            # external quotes 
            if len(row['quoted_tweet_tweetid']) > 0:
                quotes.add(row['quoted_tweet_tweetid'])

    with open(outpath_rt, 'w',newline='', encoding='utf-8') as f: 
        outwriter = csv.writer(f, delimiter=',', lineterminator=os.linesep)
        for msg in retweets:
            outwriter.writerow([msg])

    with open(outpath_rp, 'w',newline='', encoding='utf-8') as f: 
        outwriter = csv.writer(f, delimiter=',', lineterminator=os.linesep)
        for msg in replies:
            outwriter.writerow([msg])

    with open(outpath_qt, 'w',newline='', encoding='utf-8') as f: 
        outwriter = csv.writer(f, delimiter=',', lineterminator=os.linesep)
        for msg in quotes:
            outwriter.writerow([msg])
    
    print('Result for file: {}\n{:>10} external retweets\n{:>10} external replies\n{:>10} external quotes\n'.format(fname, len(retweets), len(replies), len(quotes)))
    return

def getfiles(datafolder,method=externaltweets):
    """Get all relevant data files in the subfolder of a data folder and make a list of external retweets/replies/quotes

    """
    # root folder
    problems = []
    # get all subfolders
    subfolders = [ f.path for f in os.scandir(datafolder) if f.is_dir() ]
    # get all files and parse them
    for subfolder in subfolders:
        files = [f.path for f in os.scandir(subfolder) if all([str(f).find('_tweets') > 0, os.path.splitext(f)[-1] == '.csv'])]
        for f in files:
            print('working on:  {}'.format(f))
            try:
                method(f)
            except:
                print('problem with {}'.format(f))
                problems.append(f)

    print('The following files generated an error when parsing:')
    for p in problems:
        print(p)
    return

def harvest_external_data(path):
    """from list of tweet IDs, harvest external data for network inference afterwards. Shows a progress bar for download
    """
    collected = 0
    # setup connection
    t = Twarc(CONSUMER_KEY, CONSUMER_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET, app_auth=True)
    # estimate total length
    num_lines = sum(1 for _ in open(path))
    # determine outpath
    outpath = path + '.jsonl'
    # actual hydrating
    print('working on {}'.format(path))
    with open(outpath,'w') as f:
        with tqdm.tqdm(total=num_lines) as pbar:
            for tweet in t.hydrate(open(path)):
                print(json.dumps(tweet), file=f)
                collected += 1
                pbar.update()  

    # print out difference
    with open(path + '.log', 'w') as f:
        print("Total: {}, collected {}, recovered %: {}".format(num_lines, collected, round(collected/num_lines*100,ndigits=2)), file=f)
    
def harvestfiles(datafolder):
    """Get all relevant external data files in the subfolder of a data folder and harvest them
    """
    # root folder
    problems = []
    # get all subfolders
    subfolders = [ f.path for f in os.scandir(datafolder) if f.is_dir() ]
    # get all files and parse them
    for subfolder in subfolders:
        files = [f.path for f in os.scandir(subfolder) if any([os.path.splitext(f)[-1] == '.csv_retweets',os.path.splitext(f)[-1] == '.csv_replies'])] #if str(f).endswith('.csv')]
        for f in files:
            print('working on:  {}'.format(f))
            # parsing
            try:
                harvest_external_data(f)
            except:
            #    print('problem with {}'.format(f))
                problems.append(f)

    print('The following files generated an error when parsing:')
    for p in problems:
        print(p)
    return



