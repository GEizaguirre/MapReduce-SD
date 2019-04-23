'''
Created on 22 mar. 2019

Mapping function for WordCount and CountWords utilities.
The map_dataset function is invoked from IBM Cloud Functions and it is assigned
a chunk from which generates the results.
Divides the dictionary in 40 MB subchunks and generates words iteratively to avoid
memory errors.
Partial results are put in a IBM COS bucket.
Chunk partial result notification and chunk completion messages are published into a 
IBM CloudAMQP queue.

@author: German Telmo Eizaguirre Suarez
@contact: germantelmoeizaguirre@estudiants.urv.cat
@organization: Universitat Rovira i Virgili

'''
import re
import pika
import json
from cos_backend import COSbackend

def map_dataset (map_dataset_info):
    
    '''
    Create COS session and establish a producer connection with the message queue.
    '''
    COS_session=COSbackend(map_dataset_info['cos_config'])
    dict_counter=0

    ds_range = 'bytes='+str(map_dataset_info['ds_range_min'])+'-'+str(map_dataset_info['ds_range_max'])
    
    params=pika.URLParameters(map_dataset_info['rabbit_url'])
    params.socket_timeout = 10
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
        
    '''
    Establish the first subchunk of 40 MB.
    '''
    current_min=map_dataset_info['ds_range_min']
    real_max = current_min + 41943040
    '''
    Process 40 MB subchunks one by one.
    '''
    while current_min < map_dataset_info['ds_range_max']:
        current_max = real_max
        if current_max > map_dataset_info['ds_range_max']:
            current_max = map_dataset_info['ds_range_max']
        '''
        Modelate subchunk not to cut words.
        '''
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
        
        '''
        Get subchunk.
        '''
        ds_chunk=COS_session.get_object(map_dataset_info['bucket_name'],
                                        map_dataset_info['dataset_name'],
                                        extra_get_args={'Range': ds_range_current})
        
        '''
        Get string from the subchunk with words separated by spaces.
        '''
        ds_chunk=re.sub("[^\w\s\-]", " ",  ds_chunk.decode('utf-8', 'replace'))
        result_dict = dict()
        result_dict['counting_words']=0
        result_dict['word_count']=dict()
        '''
        Generate alphanumerical words iteratively from the subchunk.
        '''
        for word in split_iter(ds_chunk):
            word.lower()
            '''
            Apply the WordCount and the CountWords.
            '''
            result_dict['counting_words'] += 1
            if word in result_dict['word_count']:
                result_dict['word_count'][word] += 1
            else: 
                result_dict['word_count'][word] = 1
                
        '''
        Put the serialized partial result of the subchunk into COS.
        '''
        serialized_result=json.dumps(result_dict)
        chunk_name='chunk' + ds_range + '_' + str(dict_counter)
        COS_session.put_object(bucket_name=map_dataset_info['chunks_bucket'], key=str(chunk_name), data=serialized_result)
        channel.basic_publish (exchange='', routing_key=map_dataset_info['queue_name'], body='chunk' + ds_range + '_' + str(dict_counter))
        dict_counter+=1
        current_min = real_max
        real_max = current_min + 41943040
       
    '''
    Publish the chunk completion message into the queue.
    ''' 
    channel.basic_publish (exchange='', routing_key=map_dataset_info['queue_name'], body='chunk' + ds_range + '_' + 'final')
    '''
    Close the producer connection with the queue.
    '''
    connection.close()
    return ({'chunk_size':ds_range})

def split_iter(string):
    '''
    Create an iterator of alphanumerical words from a string.
    '''
    return (x.group(0) for x in re.finditer(r"[^ \r\n]+", string))
    
