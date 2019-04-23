'''
Created on 22 mar. 2019

Orchestrator for mapReduce. It implements WordCount and CountWords utilities.
The orchestrator uses serverless functions from IBM Cloud Functions to share the computation
among different parallel workers. 
IBM COS is used to save the partial and final results.
IBM CloudAMQP queues are used to receive partial results messages from workers in a push model.

@author: German Telmo Eizaguirre Suarez
@contact: germantelmoeizaguirre@estudiants.urv.cat
@organization: Universitat Rovira i Virgili

'''
import sys
from cos_backend import COSbackend
from save_result import ResultLog
import ibm_botocore
import ibm_cf_connection
import pika
import json
import yaml
import time
import requests
from zipfile import ZipFile

'''
Read of the configuration file and generation
of IBM COS and IBM Clouds Functions sessions
'''
try:
    with open('cloud_config', 'r') as config_file:
        res = yaml.safe_load(config_file)
except FileNotFoundError:
    print (" We could not find your configuration yaml file cloud_config.")
    sys.exit(7)

bucket_name = res['bucket_name']
chunks_bucket=res['chunks_bucket']
COS_session = COSbackend (res['ibm_cos'])

try:
    mapping_result = ResultLog (COS_session)
except requests.exceptions.ConnectionError:
    print(" Conection to IBM cloud COS service failed. Check your connection parameters.")
    sys.exit(4)

try:    
    fn_session=ibm_cf_connection.CloudFunctions(res['ibm_cf']) 
except requests.exceptions.ConnectionError:
    print(" Conection to IBM cloud Functiones service failed. Check your connection parameters.")
    sys.exit(5)

'''
Main program of the orchestrator.
'''
def main ():
    
    chunk_number=1
    
    '''
    Control of arguments.
    '''
    if (len(sys.argv) == 1):
        print (" No parameters were detected.\n A dataset name must be specified at least. ")
        show_help()
        sys.exit(1)
        
    if sys.argv[1] == '-p':
        '''
        If printing the dictionary is selectd.
        '''
        printing = True
        if (len(sys.argv) > 3):
            chunk_number=int(sys.argv[3])
            if chunk_number > 19 :
                print (" Maximum chunk number is 20.")
                chunk_number = 19
            print(" Chunk number was set to ", chunk_number)
            if chunk_number == 0: sys.exit(0)
        else:   
            print(" Chunk number was set to the default value of ", chunk_number)
        dataset_name=sys.argv[2]
    else:
        printing = False
        if (len(sys.argv) > 2):
            chunk_number=int(sys.argv[2])
            '''
            Limit the chunk number no to exceed the message queue.
            '''
            if chunk_number > 19 :
                print (" Maximum chunk number is 20.")
                chunk_number = 19
            print(" Chunk number was set to ", chunk_number)
            if chunk_number == 0: sys.exit(0)
        else:   
            print(" Chunk number was set to the default value of ", chunk_number)
        dataset_name=sys.argv[1]
    
    dataset_list = []
    try:
        '''
        Check if the dataset exists.
        '''
        try:
            for elem in COS_session.list_objects(bucket_name):
                dataset_list.append(elem['Key']) 
        except ibm_botocore.exceptions.EndpointConnectionError: 
            print (" It seems your IBM COS configuration is not correct.")
            sys.exit(4)
        # Consult if the chosen dataset is available.
        if dataset_name in dataset_list:
            dataset_size = int(COS_session.head_object(bucket_name, dataset_name)['content-length'])
            print (" Chosen ", dataset_name, " with size ", dataset_size, "B. ")
        else:
            print (" Dataset ", dataset_name, " could not be found in bucket ", bucket_name)
            sys.exit(2)
    except ibm_botocore.exceptions.ClientError:
        print ( " Bucket ", bucket_name, " not found.")
        sys.exit(3)
     
    chunk_size=int(dataset_size/int(chunk_number))
    
    '''
    Configuration data structure for the workers.
    '''
    map_dataset_info={'cos_config':{'endpoint':res['ibm_cos']['endpoint'],
                  'secret_key':res['ibm_cos']['secret_key'],
                  'access_key':res['ibm_cos']['access_key']},
                  'bucket_name':bucket_name, 'dataset_name':dataset_name, 'chunks_bucket':chunks_bucket,
                  'rabbit_url':res['rabbit_mq']['url'],
                  'ds_range_min':None,
                  'ds_range_max':None,
                  'ds_size':dataset_size,
                  'queue_name':res['queue_name']}
    
    '''
    Check and if necessary upload the map function.
    '''
    configFunctions()
    '''
    Start RammbitAMQ queue as consumer for listening to map functions.
    '''
    channel=configQueue()
    chunk_start=0
    
    start_time=time.time()
    print (" Mapping...")
    '''
    Invoke workers one by one (as many as the chunk number).
    '''
    while chunk_start<dataset_size:
        
        chunk_end=chunk_start+chunk_size
        if chunk_end>=dataset_size: chunk_end=dataset_size - 1
                
        map_dataset_info['ds_range_min']=chunk_start
        map_dataset_info['ds_range_max']=chunk_end
        try:
            fn_session.invoke('map', map_dataset_info)
        except AttributeError:
            print ( " It seems that your IBM Cloud Functions configuration is not correct.")
            sys.exit(5)
        print (mapping_result.sent_maps)
        chunk_start=chunk_end + 1
        '''
        Control sent workers.
        '''
        mapping_result.increaseSent()
    
    '''
    Reduce results into the final dictionary.
    '''
    reduce(channel)
    print (" End of reducing")
    end_time=time.time()
    '''
    Sort the dictionary.
    '''
    mapping_result.dict['word_count'] = sorted(mapping_result.dict['word_count'].items(), key=lambda x: x[1], reverse=True)
    
    '''
    Upload serialized result to IBM COS
    '''
    serialized_result=json.dumps(mapping_result.dict)
    COS_session.put_object(bucket_name=bucket_name, key=dataset_name+'_result_'+str(chunk_number), data=serialized_result)
    print (" Results named " + dataset_name+'_result_'+str(chunk_number) + " have been uploaded to " + bucket_name)
    
    '''
    If optional printing argument-p.
    '''
    if (printing):
        print({k.encode('utf8'): v for k, v in dict(mapping_result.dict['word_count']).items()})
        
    print(" Total count of words: ", mapping_result.dict['counting_words'])
    print (" Length of dictionary: ", len(mapping_result.dict['word_count']))
    print (" Execution time for MapReduce: ", round(end_time-start_time, 4))
    return (mapping_result.dict)
 
def show_help ():
    print (" MapReduce program. Needs a chunk size (optional) and a dataset name (compulsive).")
    
def mergeDict (map_result1, map_result2, mergingFunct= lambda x,y:x):
    '''
    Merge two dictionaries into one based on a lambda function received as argument.
    Logically with an addition lambda function.
    '''
    map_result1['counting_words']+=map_result2['counting_words']
    for k,v in dict(map_result2['word_count']).items():
        if k in map_result1['word_count']:
            map_result1['word_count'][k] = mergingFunct(map_result1['word_count'][k], v)
        else:
            map_result1['word_count'][k] = v

def reduce(channel):
    '''
    Start subscription to listen to messages from the queue.
    '''
    print(" Reducing...")
    channel.start_consuming()
       
def manageResults (ch, method, properties, body):

    '''
    Check for chunk completion messages.
    '''
    if body.decode('utf-8')[-5:] == 'final':
        mapping_result.increaseReceived()
        if mapping_result.reduceEnd():
            '''
            Stop subscription.
            '''
            ch.stop_consuming()
    else: 
        '''
        Get partial result name, read it from COS and merge it.
        '''
        chunk_dict = COS_session.get_object (chunks_bucket, body.decode('utf-8'))
        chunk_dict = json.loads(chunk_dict)
        print(mapping_result.received_maps, body.decode('utf-8'))
        mergeDict (mapping_result.dict, chunk_dict, lambda n1,n2: n1+n2)
        '''
        Delete partial result object.
        '''
        COS_session.delete_object(chunks_bucket, body.decode('utf-8'))
    
def configQueue():
    '''
    Configurate message queue RabbitAMQ as consumer.
    '''
    params = pika.URLParameters(res['rabbit_mq']['url'])
    try:
        connection = pika.BlockingConnection(params)
    except pika.exceptions.ConnectionClosedByBroker:
        print (" It seems your Rabbit MQ url is not correct")
        sys.exit(6)
    channel = connection.channel()
    '''
    Stablish queue name.
    '''
    channel.queue_declare(queue=res['queue_name'])
    channel.basic_consume(res['queue_name'], manageResults, True)
    return channel
    
def configFunctions():
    '''
    If mapping function does not exist, zip necessary files and put it on
    IBM Cloud Functions.
    '''
    if ('error' in dict(fn_session.get_action('map'))) : 
        print(" The map serverless function does not exist and will be installed.")
        with ZipFile('md.zip','w') as zp: 
        # writing each file one by one 
            zp.write('__main__.py')
            zp.write('cos_backend.py')
            zp.write('map_dataset.py')
        
        with open('md.zip', 'rb') as file_data:
            z = file_data.read()
            
        fn_session.create_action('map',z, 'python:3.6')
        
    else: print (" The map serverless function is already installed in IBM Cloud.")
    
if __name__ == "__main__":
    main() 
    

    

    
    
    


