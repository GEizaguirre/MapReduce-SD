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
    
    # Modelate chunk not to cut words 
    first_char=map_dataset_info['ds_range_min']
    ds_char='bytes='+str(first_char)+'-'+str(first_char)
    while first_char > 0 and COS_session.get_object(map_dataset_info['bucket_name'],
                                        map_dataset_info['dataset_name'],
                                        extra_get_args={'Range': ds_char}).decode('utf-8').isalnum():
        first_char -= 1
        ds_char='bytes='+str(first_char)+'-'+str(first_char)
        
    last_char=map_dataset_info['ds_range_max']
    if last_char < map_dataset_info['ds_size'] - 1:
        ds_char='bytes='+str(last_char)+'-'+str(last_char)
        while last_char > first_char and COS_session.get_object(map_dataset_info['bucket_name'],
                                            map_dataset_info['dataset_name'],
                                            extra_get_args={'Range': ds_char}).decode('utf-8').isalnum():
            last_char -= 1
            ds_char='bytes='+str(last_char)+'-'+str(last_char)
    
    ds_range = ds_char='bytes='+str(first_char)+'-'+str(last_char)
    
    ds_chunk=COS_session.get_object(map_dataset_info['bucket_name'],
                                    map_dataset_info['dataset_name'],
                                    extra_get_args={'Range': ds_range})
    
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
    chunk_name='chunk' + ds_range
    COS_session.put_object(bucket_name=map_dataset_info['bucket_name'], key=str(chunk_name), data=serialized_result)
    
    params=pika.URLParameters(map_dataset_info['rabbit_url'])
    params.socket_timeout = 10
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    
    channel.basic_publish (exchange='', routing_key='mapReduceSD', body='chunk' + ds_range)
    connection.close()
    return ({'chunk_name':chunk_name})


    
