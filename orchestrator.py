'''
Created on 13 mar. 2019

@author: German
'''
import sys
from configuration_paramethers import bucket_name, functions_config, rabbit_config
from cos_backend import COSbackend
import ibm_botocore
import ibm_cf_connection
import configure_functions
import configuration_paramethers
import pika
import json

# Default chunk size
chunk_number=1
sent_maps=None
global_dict = dict()
global_dict['counting_words'] = 0
global_dict['word_count'] = dict()
COS_session=COSbackend(configuration_paramethers.cos_config)

def main ():
    
    
    if (len(sys.argv) == 1):
        print (" No parameters were detected.\n A dataset name must be specified at least. ")
        show_help()
        exit
    if (len(sys.argv) > 2):
        chunk_number=sys.argv[2]
        print(" Chunk number was set to ", chunk_number)
        if chunk_number == 0: exit
    else:   
        print(" Chunk number was set to the default value of ", chunk_number)
    dataset_name=sys.argv[1]
    
    dataset_list = []
    try:
        # Create list of datasets.
        for elem in COS_session.list_objects(bucket_name):
            dataset_list.append(elem['Key'])  
        # Consult if the chosen dataset is available.
        if dataset_name in dataset_list:
            dataset_size = COS_session.head_object(bucket_name, dataset_name)['content-length']
            print (" Chosen ", dataset_name, " with size ", dataset_size, "B. ")
        else:
            print (" Dataset ", dataset_name, " could not be found in bucket ", bucket_name)
        print ( " Processing... ")
    except ibm_botocore.exceptions.ClientError:
        print ( " Bucket ", bucket_name, " not found.")
     
    dataset_size=658
    chunk_size=int(dataset_size/int(chunk_number))

    chunk_start=0
    
    map_dataset_info={'cos_config':{'endpoint':configuration_paramethers.cos_config['endpoint'],
                  'secret_key':configuration_paramethers.cos_config['secret_key'],
                  'access_key':configuration_paramethers.cos_config['access_key']},
                  'bucket_name':bucket_name, 'dataset_name':dataset_name,
                  'rabbit_url':rabbit_config['url'],
                  'ds_range':None}
    fn_session=ibm_cf_connection.CloudFunctions(functions_config)
    global global_dict
    
    # Start RabbitMQ queue for listening to map functions
    channel=configQueue()
    global sent_maps
    sent_maps=0
    while chunk_start<dataset_size:
        
        chunk_end=chunk_start+chunk_size
        #print ("range: ", chunk_start, chunk_end)
        if chunk_end>=dataset_size: chunk_end=dataset_size - 1
        else:
            ds_char='bytes='+str(chunk_end)+'-'+str(chunk_end)
            
            while (chunk_end<dataset_size) and (COS_session.get_object(bucket_name,
                                        dataset_name,
                                        extra_get_args={'Range': ds_char})).decode('utf-8').isalnum():
                chunk_end+=1
                ds_char='bytes='+str(chunk_end)+'-'+str(chunk_end)
        
            chunk_end-=1
                
        print ("range: ", chunk_start, chunk_end)
        ds_range='bytes='+str(chunk_start)+'-'+str(chunk_end)
        map_dataset_info['ds_range']=ds_range
        #chunk_dict=fn_session.invoke_with_result('mapDataset', map_dataset_info)
        print(fn_session.invoke_with_result('map', map_dataset_info))
        #print (chunk_dict)
        #global_dict=mergeDict(global_dict, chunk_dict, lambda n1,n2: n1+n2)
        # llamar a funcion
        chunk_start=chunk_end + 1
        sent_maps=sent_maps+1
    
    global received_maps
    received_maps=0
    reduce(channel)
    print ("End of mapping")
    print (global_dict)
    #global_dict_sorted = sorted(global_dict.items(), key=lambda x: x[1], reverse=True)
    #print (global_dict_sorted)
    #return (global_dict_sorted)
 
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
    print("Accumulating maps")
    channel.start_consuming()
       
def manageResults (ch, method, properties, body):
    # gestionar contenido de body llamando a mergeDict
    global received_maps
    global global_dict
    chunk_dict = COS_session.get_object (bucket_name, body.decode('utf-8'))
    chunk_dict = json.loads(chunk_dict)
    #print (chunk_dict)
    mergeDict (global_dict, chunk_dict, lambda n1,n2: n1+n2)
    COS_session.delete_object(bucket_name, body.decode('utf-8'))
    received_maps += 1
    if received_maps == sent_maps:
        ch.stop_consuming()
    
def configQueue():
    params = pika.URLParameters(rabbit_config['url'])
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='mapReduceSD')
    channel.basic_consume('mapReduceSD', manageResults, True)
    return channel
    
if __name__ == "__main__":
    main() 
    

    

    
    
    


