'''
Created on 13 mar. 2019

@author: German
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

try:
    with open('cloud_config', 'r') as config_file:
        res = yaml.safe_load(config_file)
except FileNotFoundError:
    print (" We could not find your configuration yaml file cloud_config.")
    sys.exit(7)

bucket_name = res['bucket_name']
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

def main ():
    
    chunk_number=1
    
    if (len(sys.argv) == 1):
        print (" No parameters were detected.\n A dataset name must be specified at least. ")
        show_help()
        sys.exit(1)
        
    if sys.argv[1] == '-p':
        printing = True
        if (len(sys.argv) > 3):
            chunk_number=int(sys.argv[3])
            if chunk_number > 20 :
                print (" Maximum chunk number is 20.")
                chunk_number = 20
            print(" Chunk number was set to ", chunk_number)
            if chunk_number == 0: sys.exit(0)
        else:   
            print(" Chunk number was set to the default value of ", chunk_number)
        dataset_name=sys.argv[2]
    else:
        printing = False
        if (len(sys.argv) > 2):
            chunk_number=int(sys.argv[2])
            if chunk_number > 20 :
                print (" Maximum chunk number is 20.")
                chunk_number = 20
            print(" Chunk number was set to ", chunk_number)
            if chunk_number == 0: sys.exit(0)
        else:   
            print(" Chunk number was set to the default value of ", chunk_number)
        dataset_name=sys.argv[1]
    
    dataset_list = []
    try:
        # Create list of datasets.
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
    
    map_dataset_info={'cos_config':{'endpoint':res['ibm_cos']['endpoint'],
                  'secret_key':res['ibm_cos']['secret_key'],
                  'access_key':res['ibm_cos']['access_key']},
                  'bucket_name':bucket_name, 'dataset_name':dataset_name,
                  'rabbit_url':res['rabbit_mq']['url'],
                  'ds_range_min':None,
                  'ds_range_max':None,
                  'ds_size':dataset_size}
    
    
    # Start RabbitMQ queue for listening to map functions
    channel=configQueue()
    chunk_start=0
    
    start_time=time.time()
    print (" Mapping...")
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
        mapping_result.increaseSent()
    
    reduce(channel)
    print (" End of mapping")
    end_time=time.time()
    mapping_result.dict['word_count'] = sorted(mapping_result.dict['word_count'].items(), key=lambda x: x[1], reverse=True)
    #print (mapping_result.dict)
    # NEW 
    if (printing):
        print({k.encode('utf8'): v for k, v in dict(mapping_result.dict['word_count']).items()})
    print(" Total count of words: ", mapping_result.dict['counting_words'])
    print (" Length of dictionary: ", len(mapping_result.dict['word_count']))
    print (" Execution time for MapReduce: ", round(end_time-start_time, 4))
    return (mapping_result.dict)
 
def show_help ():
    print (" MapReduce program. Needs a chunk size (optional) and a dataset name (compulsive).")
    
def mergeDict (map_result1, map_result2, mergingFunct= lambda x,y:x):
    map_result1['counting_words']+=map_result2['counting_words']
    for k,v in dict(map_result2['word_count']).items():
        if k in map_result1['word_count']:
            map_result1['word_count'][k] = mergingFunct(map_result1['word_count'][k], v)
        else:
            map_result1['word_count'][k] = v

def reduce(channel):
    print(" Reducing...")
    channel.start_consuming()
       
def manageResults (ch, method, properties, body):

    if body.decode('utf-8')[-5:] == 'final':
        mapping_result.increaseReceived()
        # que manden mensaje al acabar
        if mapping_result.reduceEnd():
            ch.stop_consuming()
    else: 
        chunk_dict = COS_session.get_object (bucket_name, body.decode('utf-8'))
        chunk_dict = json.loads(chunk_dict)
        print(mapping_result.received_maps, body.decode('utf-8'))
        mergeDict (mapping_result.dict, chunk_dict, lambda n1,n2: n1+n2)
        COS_session.delete_object(bucket_name, body.decode('utf-8'))
    
def configQueue():
    params = pika.URLParameters(res['rabbit_mq']['url'])
    try:
        connection = pika.BlockingConnection(params)
    except pika.exceptions.ConnectionClosedByBroker:
        print (" It seems your Rabbit MQ url is not correct")
        sys.exit(6)
    channel = connection.channel()
    channel.queue_declare(queue='mapReduceSD')
    channel.basic_consume('mapReduceSD', manageResults, True)
    return channel
    
if __name__ == "__main__":
    main() 
    

    

    
    
    


