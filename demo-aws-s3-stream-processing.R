library(aws.s3)
library(ndjson)
library(foreach)
library(tidyverse)

# set environment variables for AWS S3 connection, these could be set exeternally
Sys.setenv(
  "AWS_ACCESS_KEY_ID" = "YOUR_ACCES_KEY",
  "AWS_SECRET_ACCESS_KEY" = "YOUR_SECRET_KEY",
  "AWS_DEFAULT_REGION" = "us-east-1", # change this to your AWS S3 region if it's not us-east-1

  # leave this off if you aren't using minio as your s3 storage locally
  "AWS_S3_ENDPOINT" = "127.0.0.1:9000"
)

# processModerationFile
#
# function to process each gzipped ndjson file of reddit moderations
# returns the parsed ndjson data as a data table
# 
processModerationFile <- function(path_to_S3_data_item) {
  print(path_to_S3_data_item)
  moderations <- stream_in(path_to_S3_data_item, c('dt'))
  moderations
}

# processModerationYear
#
# function to process the reddit moderations for a year
# returns a summary of the moderations for that year
#
processModerationYear <- function(moderations, file_year) {
  moderations %>% 
    mutate (
      year = file_year
    ) %>% group_by(year, action, mod) %>%
    summarise(num_actions = n())
}

# the name of the s3 bucket
item_bucket <- "reddit-conspiracy-data"
# list the files in the bucket
yearly_conspiracy_files <- get_bucket(item_bucket, use_https = FALSE, region ="")

# loop over the files in the bucket and row bind the results of each loop iteration
moderation_summary <- foreach(j=1:length(yearly_conspiracy_files), .combine = rbind) %do% {
    
  # get the s3 item for this iteration
  s3_item = yearly_conspiracy_files[[j]]
  # construct a full path for the item
  s3_full_path = stringr::str_interp("s3://${s3_item$Bucket}/${s3_item$Key}")
  # extract the year from the first 4 characters of the file name
  file_year = stringr::str_sub(s3_item$Key, 1,4)
  
  # read the S3 data for this s3 object and pass the data to the specified function (processModerationFile)
  mod_data <- s3read_using(FUN=processModerationFile,object=s3_full_path, 
  # you can leave these "opts" out if you are not using minio as your local S3 storage
  opts= c(use_https = FALSE, region = ""))
  
  # process the received data, summarise and return
  processModerationYear(mod_data, file_year)
}