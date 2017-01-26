import os
import pandas as pd
import json

OUTPUT_DIR = 'data/ap'

def projector_config(user_topics):
    projector_config_dict = {
        'embeddings': [
        {
            "metadataPath": "https://raw.githubusercontent.com/pilipolio/nlp-media-hack/master/data/ap/user_metadata.tsv",
            'tensorName': 'User average topics',
            'tensorShape': user_topics.shape,
            'tensorPath': 'https://raw.githubusercontent.com/pilipolio/nlp-media-hack/master/data/ap/user_topics.tsv'
        }
        ]
    }
    return projector_config_dict

users = pd.read_csv('data/ap/users.csv').iloc[:, 1:]
print('loaded {} users'.format(users.shape[0]))

user_topics = pd.read_csv('data/ap/user_topics.csv', sep=',').iloc[:, 1:]
print('loaded {} user_topics'.format(user_topics.shape))

## dump
user_topics.to_csv(os.path.join(OUTPUT_DIR, 'user_topics.tsv'), sep='\t', index=None, header=None, float_format='%.6f')

with open(os.path.join(OUTPUT_DIR, 'user_projector_config.json'), 'w') as f:
    json.dump(projector_config(user_topics), f)

users.to_csv(os.path.join(OUTPUT_DIR, 'user_metadata.tsv'), sep='\t')
