import os
import pandas as pd
import json

OUTPUT_DIR = 'data/ap'

def projector_config(article_topics):
    projector_config_dict = {
        'embeddings': [
        {
            "metadataPath": "https://raw.githubusercontent.com/pilipolio/nlp-media-hack/master/data/ap/article_metadata.tsv",
            'tensorName': 'Article topics',
            'tensorShape': article_topics.shape,
            'tensorPath': 'https://raw.githubusercontent.com/pilipolio/nlp-media-hack/master/data/ap/article_topics.tsv'
        }
        ]
    }
    return projector_config_dict

articles = pd.read_csv('data/ap/articles.csv')\
           .assign(category=lambda df: df.categories.str[1:-1].str.split(',').str[0])

print('loaded {} articles'.format(articles.shape[0]))

article_topics = pd.read_csv('data/ap/article_topics.csv', sep=',')\
                   .set_index('id')
print('loaded {} article_topics'.format(article_topics.shape))

## dump
article_topics.to_csv(os.path.join(OUTPUT_DIR, 'article_topics.tsv'), sep='\t', index=None, header=None)

with open(os.path.join(OUTPUT_DIR, 'projector_config.json'), 'w') as f:
    json.dump(projector_config(article_topics), f)

articles[['title', 'category']].to_csv(os.path.join(OUTPUT_DIR, 'article_metadata.tsv'), sep='\t')
