from pyspark.sql import Row
import re

# lower case captions
def lower_case(row):
    lowered_caption = row['caption'].lower()
    lowered_shrtcaption = row['short_caption'].lower()
    return Row(_id=row['_id'], 
                caption=lowered_caption, 
                created_time=row['created_time'], 
                howpublished=row['howpublished'],
                publisher=row['publisher'], 
                short_caption=lowered_shrtcaption, 
                url=row['url'], year=row['year'])

def remove_punctuations(row):
    caption = re.sub(r'[^\w\d\s]', '', row['caption'])
    shrt_caption = re.sub(r'[^\w\d\s]', '', row['short_caption'])
    return Row(_id=row['_id'],
                caption=caption, 
                created_time=row['created_time'], 
                howpublished=row['howpublished'],
                publisher=row['publisher'], 
                short_caption=shrt_caption, 
                url=row['url'], year=row['year'])
    
def tokenize(row):
    caption = row['caption'].split()
    shrt_caption = row['short_caption'].split()
    return Row(_id=row['_id'],
                caption=caption, 
                created_time=row['created_time'], 
                howpublished=row['howpublished'],
                publisher=row['publisher'], 
                short_caption=shrt_caption, 
                url=row['url'], year=row['year'])
    
def test():
    print("Hello")