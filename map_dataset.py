'''
Created on 9 abr. 2019

@author: German
'''
import re
import pika
import json
from cos_backend import COSbackend

def map_dataset (map_dataset_info):
    
    COS_session=COSbackend(map_dataset_info['cos_config'])
    ds_chunk=COS_session.get_object(map_dataset_info['bucket_name'],
                                    map_dataset_info['dataset_name'],
                                    extra_get_args={'Range': map_dataset_info['ds_range']})
    
    ds_chunk=re.sub("[^\w\s\-]", " ",  ds_chunk.decode('utf-8'))
    result_dict = dict()
    result_dict['counting_words']=0
    result_dict['word_count']=dict()
    ds_list=ds_chunk.lower().split()
    for word in ds_list:
        result_dict['counting_words'] += 1
        if word in result_dict['word_count']:
            result_dict['word_count'][word] += 1
        else: result_dict['word_count'][word] = 1  
    
    serialized_result=json.dumps(result_dict)
    chunk_name='chunk' + map_dataset_info['ds_range']
    COS_session.put_object(bucket_name=map_dataset_info['bucket_name'], key=str(chunk_name), data=serialized_result)
    
    params=pika.URLParameters(map_dataset_info['rabbit_url'])
    params.socket_timeout = 10
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    
    channel.basic_publish (exchange='', routing_key='mapReduceSD', body='chunk' + map_dataset_info['ds_range'])
    return ({'chunk_name':chunk_name})


    
