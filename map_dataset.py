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
    dict_counter=0

    ds_range = 'bytes='+str(map_dataset_info['ds_range_min'])+'-'+str(map_dataset_info['ds_range_max'])
    
    params=pika.URLParameters(map_dataset_info['rabbit_url'])
    params.socket_timeout = 10
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
        
    current_min=map_dataset_info['ds_range_min']
    real_max = current_min + 41943040 
    while current_min < map_dataset_info['ds_range_max']:
        current_max = real_max
        if current_max > map_dataset_info['ds_range_max']:
            current_max = map_dataset_info['ds_range_max']
        # Modelate chunk not to cut words 
        ds_char='bytes='+str(current_min)+'-'+str(current_min)
        while current_min > 0 and COS_session.get_object(map_dataset_info['bucket_name'],
                                            map_dataset_info['dataset_name'],
                                            extra_get_args={'Range': ds_char}).decode('utf-8', 'replace').isalnum():
            current_min -= 1
            ds_char='bytes='+str(current_min)+'-'+str(current_min)
            
        if current_max < map_dataset_info['ds_size'] - 1:
            ds_char='bytes='+str(current_max+1)+'-'+str(current_max+1)
            if COS_session.get_object(map_dataset_info['bucket_name'],
                                                    map_dataset_info['dataset_name'],
                                                    extra_get_args={'Range': ds_char}).decode('utf-8', 'replace').isalnum():
                ds_char='bytes='+str(current_max)+'-'+str(current_max)
                while current_max > current_min and COS_session.get_object(map_dataset_info['bucket_name'],
                                                    map_dataset_info['dataset_name'],
                                                    extra_get_args={'Range': ds_char}).decode('utf-8', 'replace').isalnum():
                    current_max -= 1
                    ds_char='bytes='+str(current_max)+'-'+str(current_max)
        
        ds_range_current = 'bytes='+str(current_min)+'-'+str(current_max)
        
        ds_chunk=COS_session.get_object(map_dataset_info['bucket_name'],
                                        map_dataset_info['dataset_name'],
                                        extra_get_args={'Range': ds_range_current})
        
        ds_chunk=re.sub("[^\w\s\-]", " ",  ds_chunk.decode('utf-8', 'replace'))
        result_dict = dict()
        result_dict['counting_words']=0
        result_dict['word_count']=dict()
        #ds_list=ds_chunk.lower().split()
        for word in split_iter(ds_chunk):
            word.lower()
            result_dict['counting_words'] += 1
            if word in result_dict['word_count']:
                result_dict['word_count'][word] += 1
            else: 
                result_dict['word_count'][word] = 1 
            #if (len(result_dict['word_count']) == 100000):
            #    serialized_result=json.dumps(result_dict)
            #    chunk_name='chunk' + ds_range + '_' + str(dict_counter)
            #    COS_session.put_object(bucket_name=map_dataset_info['chunks_bucket'], key=str(chunk_name), data=serialized_result)
            #    channel.basic_publish (exchange='', routing_key='mapReduceSD', body='chunk' + ds_range + '_' + str(dict_counter))
            #    result_dict['word_count']={}
            #    result_dict['counting_words']=0
            #    dict_counter+=1
                
        serialized_result=json.dumps(result_dict)
        chunk_name='chunk' + ds_range + '_' + str(dict_counter)
        COS_session.put_object(bucket_name=map_dataset_info['chunks_bucket'], key=str(chunk_name), data=serialized_result)
        channel.basic_publish (exchange='', routing_key=map_dataset_info['queue_name'], body='chunk' + ds_range + '_' + str(dict_counter))
        dict_counter+=1
        current_min = real_max
        real_max = current_min + 41943040
        
    channel.basic_publish (exchange='', routing_key=map_dataset_info['queue_name'], body='chunk' + ds_range + '_' + 'final')
    connection.close()
    return ({'chunk_size':ds_range})

def split_iter(string):
    #return (x.group(0) for x in re.finditer(r"[0-9a-z']+", string))
    return (x.group(0) for x in re.finditer(r"[^ \r\n]+", string))
    
