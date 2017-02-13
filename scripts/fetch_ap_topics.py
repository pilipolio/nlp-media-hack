
# coding: utf-8

# In[1]:

topicsPath = 's3://schibsted-spt-common-dev/fredrik/green/aftenposten/content-topics/version=3/lookback=2000/year=2016/month=5/day=3/lookback=400/year=2017/month=1/day=10/'

topics = sqlContext.read.parquet(topicsPath).cache()

print(topics.count())
topics.columns


# In[2]:

topics.select('id', 'topicDistribution').take(1)


# In[3]:

article_topics = topics.select('id', 'topicDistribution').toPandas()


# In[4]:

article_topics.head()


# In[22]:

import numpy as np
import pandas as pd

article_topic_arrays = pd.DataFrame(
    index=article_topics.id,
    data=np.array(article_topics.topicDistribution.apply(np.array).values.tolist()))

article_topic_arrays.head(3)


# In[5]:

content_path = 's3://schibsted-spt-common-dev/staging/green/aftenposten-v3-smpcreate/content/daily/2016/12'
article_metadata = sqlContext.read.json(content_path).cache()
article_metadata.columns


# In[23]:

articles = article_metadata.select('id', 'title', 'image-url', 'categories', 'tags').toPandas()

print(articles.shape)
articles.head()


# In[34]:

print(articles.id.isin(article_topics.id).sum())
print(article_topic_arrays.index.isin(articles.id).sum())

# DUMP
articles[articles.id.isin(article_topic_arrays.index)].set_index('id').sort_index().to_csv('articles.csv', encoding='utf-8')
article_topic_arrays[article_topic_arrays.index.isin(articles.id)].sort_index().to_csv('article_topics.csv', encoding='utf-8')


# In[32]:

article_topic_arrays.head()


# In[ ]:



