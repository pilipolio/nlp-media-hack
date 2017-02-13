
# coding: utf-8

# In[9]:

content_path = 's3://schibsted-spt-common-dev/staging/green/aftenposten-v3-smpcreate/content/daily/2016/12'
article_metadata = sqlContext.read.json(content_path).cache()
article_metadata.columns


# In[31]:

import numpy as np
np.array(article_topics.topicDistribution[0])

article_topics = article_topics.assign(
    topicDistribution=lambda df: df.topicDistribution.apply(np.array))

article_topics.head()


# In[1]:

userTopics = sqlContext.read.parquet('s3://schibsted-spt-analytics-dev/oslo-nlp-um/aftenposten_sum_topics_2/2016-12/').cache()

print(userTopics.count())
userTopics.columns


# In[2]:

userAccounts = sqlContext.read.parquet('s3://schibsted-spt-analytics-dev/oslo-nlp-um/no-accounts/2016-12').cache()

print(userAccounts.count())

userAccounts.columns


# In[3]:

userTopics.take(1)


# In[4]:

userTopics.where(userTopics['isAnonymous'] == False)


# In[ ]:

userTopics.join()


# In[5]:

userFeatures = sqlContext.read.parquet("s3://schibsted-spt-analytics-dev/oslo-nlp-um/aftenposten/all_features/2016-12/").cache()

print(userFeatures.count())

userFeatures.columns


# In[16]:

user_features = userFeatures    .filter(userFeatures.genderLabel.isNotNull())    .filter(userFeatures.ageLabel.isNotNull())    .select('avgTopic', 'genderLabel', 'ageLabel', 'numEventsTotal')    .toPandas()


# In[17]:

user_features.head()


# In[33]:

import numpy as np
import pandas as pd

user_features = user_features.query('numEventsTotal > 500')

print(user_features.shape)

user_topics = pd.DataFrame(
    data=np.array(user_features.avgTopic.apply(np.array).values.tolist()))

user_topics.to_csv('user_topics.csv', encoding='utf-8')

print(user_topics.shape)

user_topics.head(3)


# In[32]:

user_metadata = user_features[['genderLabel', 'ageLabel', 'numEventsTotal']]

user_metadata.to_csv('users.csv', encoding='utf-8')
print(user_metadata.shape)
user_metadata.head()


# In[31]:

print(1)


# In[ ]:



