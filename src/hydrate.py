#
#  script to identify external interactions for a given dataset
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

from hydratorpipeline import harvestfiles, harvest_external_data

# harvesting the external files for Twitter information operations dataset
datapath = './demodata'
harvestfiles(datapath)

# harvesting the tweets for Plandemic dataset
datapath = './demodata/dataset_A/externaltweets.csv_replies'
harvest_external_data(datapath)

