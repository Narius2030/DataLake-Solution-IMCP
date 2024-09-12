from pyspark.sql.functions import col, lower, lit
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, ArrayType
from datetime import datetime
import re

def lower_case(input_df):
    temp_lwc = input_df.withColumn('caption', lower(col('caption')))
    temp_lwc = temp_lwc.withColumn('short_caption', lower(col('short_caption')))
    return temp_lwc
    
def remove_punctuations(input_df):
    remove_punc_udf = F.udf(lambda text: re.sub(r'[^a-zA-Z\s]', '', text), StringType())
    temp_rmp = input_df.withColumn('caption', remove_punc_udf(col('caption')))
    temp_rmp = temp_rmp.withColumn('short_caption', remove_punc_udf(col('short_caption')))
    return temp_rmp
    
def tokenize_caption(input_df):
    tokenize_udf = F.udf(lambda text: text.split(" "), ArrayType(StringType()))
    temp_tok = input_df.withColumn('caption_tokens', tokenize_udf(col('caption')))
    temp_tok = temp_tok.withColumn('short_caption_tokens', tokenize_udf(col('short_caption')))
    return temp_tok

def scaling(input_df):
    temp_scale = input_df.withColumn('created_time', lit(datetime.now()))
    temp_scale = temp_scale.select(['url', 'caption', 'short_caption', 'caption_tokens', 'short_caption_tokens', 'publisher', 'created_time'])
    return temp_scale