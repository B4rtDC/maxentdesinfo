This repository contains the code associated with the paper "Maximum entropy networks applied on Twitter disinformation datasets". The scripts are written in Julia (>= 1.6) and Python (>=3.8).

To use the different scripts, you need to have downloaded the archives from the [Twitter information operations repository](https://transparency.twitter.com/en/reports/information-operations.html) of from [George Washingtons's TweetSets](https://tweetsets.library.gwu.edu).

The different steps to undertake for each dataset in the Twitter information operations repository are the following:
1. extract the external tweets that need to be downloaded
2. download the external tweets
3. build the interaction network(s)
4. identify the significant interactions and project the interaction network on the user layer
5. analyze the results

For the Plandemic tweets, only steps 3-5 are required.

# Twitter information operation report
## Data preparation
1. Download the files associated with a specific dataset. This will give you a set of files that holds the tweets and the user info that has the following naming conventions:
    ```julia
    dataset_users_csv_hashed.csv # holds the flagged user info
    dataset_tweets_csv_hashed_XX.csv # holds the tweets, can be multiple files indicated by the number XX
    ```
2. From the Analyse module, run the function `get_externals`. This will provide two files, one for the external retweets and one for the external replies to be downloaded.
## Obtaining external tweets
1. Set your credentials for the Twitter API in `credentials.py`
2. Run the the function `harvestfiles` from `hydratorpipeline.py`. This will identify the files that hold the external data. The tweets will be downloaded into a .jsonl file. At the same time, a log file will be generate to track what percentage of message could be recovered.
## Building the network
We now have all required date to run the analysis. If you have a folder that holds all
1. The function `grapher` will build the different graphs (interaction graph, bipartite graph & projected bipartite graph)
2. Run community detection on the different graphs

## Exporting the network to Gephi
You can export each network for importation in Gephi by exporting the nodelist and edgelist

# Plandemic dataset
## Data preparation
1. Run the query on the [George Washington TweetSet](https://tweetsets.library.gwu.edu) in order to obtain the tweet ids to download
2. Set your credentials for the Twitter API in credentials.py
2. Run the the function `harvest_external_data` from `hydratorpipeline.py` on each file holding tweetids. The tweets will be downloaded into a .jsonl file. At the same time, a log file will be generate to track what percentage of message could be recovered.

## Building the network
1. Run the function `makegraphs`. This will generate the different graphs (interaction graph, bipartite graph & projected bipartite graph)
2. Run community detection on the different graphs
## Exporting the network to Gephi
You can export each network for importation in Gephi by exporting the nodelist and edgelist