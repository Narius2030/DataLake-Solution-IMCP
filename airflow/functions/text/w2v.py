from pyspark.sql import Row
from tqdm import tqdm
import tensorflow as tf


def create_sentences(bronze_rdd):
    sentences = []
    for row in tqdm(bronze_rdd.collect()):
        sentences += [row['caption'], row['short_caption']]
    return sentences

def create_corpus(sentences):
    # filter several punctuations in sentences
    tokenizer = tf.keras.preprocessing.text.Tokenizer(filters='!“"”#$%&()*+,-./:;<=>?@[\]^`{|}~ ')
    # create vocabulary (corpus) for each word in N sentences
    tokenizer.fit_on_texts(sentences)
    # map word to token in corpus
    return dict(tokenizer.word_index)


def w2v_corpus(row, corpus):
    sentences = [row['caption'], row['short_caption']]
    tokens= []
    for sentence in sentences:
        words = sentence.split()
        tokens.append([corpus[word] for word in words])
    return Row(_id=row['_id'], 
                caption=tokens[0], 
                created_time=row['created_time'], 
                howpublished=row['howpublished'],
                publisher=row['publisher'],
                short_caption=tokens[1], 
                url=row['url'], year=row['year'])
