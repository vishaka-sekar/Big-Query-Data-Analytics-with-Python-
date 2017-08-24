# Vishaka Balasubramanian Sekar
# Data Analysis on Large Datasets

import json
import os
import math
import linecache
import sys
from pandas import io

median = []
mean=[]
old_median=0.0
new_median=0.0
old_mean = 0.0
new_mean= 0.0



# put your project ID here
project_id = "yourprojectID"
#  query here
query = "SELECT MESSAGE FROM  [bigquery-public-data:github_repos.commits] WHERE TIMESTAMP(committer.date) > DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -6, 'HOUR') LIMIT 10"
df = io.gbq.read_gbq(query, project_id=project_id)

n  =  len(df.index) #total records in current dataset
for row in df.iterrows():

    median.append(row[1].str.len())
    mean.append(row[1].str.len())

mid = len(median)/2
new_median = median[math.floor(mid)]
new_mean = sum(mean) / n
print('mean of : ',new_mean,'characters')
print('median of :',new_median,'characters')







