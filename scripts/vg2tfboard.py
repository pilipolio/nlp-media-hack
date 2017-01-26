import os
import pandas as pd
import json

OUTPUT_DIR = 'data/vg'

def projector_config(article_topics):
    projector_config_dict = {
        'embeddings': [
        {
            "metadataPath": "https://raw.githubusercontent.com/pilipolio/nlp-media-hack/master/data/vg/article_metadata.tsv",
            'tensorName': 'Article topics',
            'tensorShape': article_topics.shape,
            'tensorPath': 'https://raw.githubusercontent.com/pilipolio/nlp-media-hack/master/data/vg/article_topics.tsv'
        }
        ]
    }
    return projector_config_dict

all_articles = pd.read_csv('data/vg/metadata_article.txt', sep=';', encoding='utf-8')\
    .assign(pub_date=lambda df: pd.to_datetime(df.pub_date))\
    .assign(title = lambda df:df.title.replace({'\n': '|'}, regex=True))

articles = all_articles.query("pub_date >= '2016-12-01'")
articles.article_id = articles.article_id.astype(int)
articles = articles.set_index('article_id')

print('loaded {} article and selecting {}'.format(all_articles.shape[0], articles.shape[0]))

article_topics = pd.read_csv('data/vg/topic.txt', sep=';').set_index('id')
print('loaded {} article_topics'.format(article_topics.shape))

# aligning articles and topics
article_topics = article_topics[article_topics.index.isin(articles.index)].sort_index()
articles = articles[articles.index.isin(article_topics.index)].sort_index()

print('{}/{} common article/topics'.format(articles.shape[0], article_topics.shape[0]))

## dump
article_topics.to_csv(os.path.join(OUTPUT_DIR, 'article_topics.tsv'), sep='\t', index=None, header=None)

with open(os.path.join(OUTPUT_DIR, 'projector_config_2.json'), 'w') as f:
    json.dump(projector_config(article_topics), f)

articles[['title', 'categorie', 'pub_date']].to_csv(os.path.join(OUTPUT_DIR, 'article_metadata.tsv'), sep='\t', encoding='utf-8')
