'''
Created on 13 mar. 2019

@author: German
'''
import sys
from configuration_paramethers import bucket_name, functions_config
from cos_backend import COSbackend
import ibm_botocore
import ibm_cf_connection
import configure_functions
import configuration_paramethers
import chunk

''' Default chunk size '''
chunk_size=1

def main ():
    
    if (len(sys.argv) == 1):
        print (" No paramethers were detected.\n A dataset name must be specified at least. ")
        show_help()
        exit
    if (len(sys.argv) > 2):
        chunk_size=sys.argv[2]
        print(" Chunk size was set to ", chunk_size, "MB")
        dataset_name=sys.argv[1]
    else:   
        chunk_size=1
        print(" Chunk size was set to the default value of ", chunk_size, "MB")
        dataset_name=sys.argv[1]
    
    COS_session=COSbackend(configuration_paramethers.cos_config)
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
     
    chunk_size=15  
    dataset_size=658
    chunk_start=0
    
    map_dataset_info={'cos_config':{'endpoint':configuration_paramethers.cos_config['endpoint'],
                  'secret_key':configuration_paramethers.cos_config['secret_key'],
                  'access_key':configuration_paramethers.cos_config['access_key']},
                  'bucket_name':bucket_name, 'dataset_name':dataset_name,
                  'ds_range':None}
    fn_session=ibm_cf_connection.CloudFunctions(functions_config)
    global_dict={'word_counting':0, 'word_count':dict()}
    
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
        chunk_dict=fn_session.invoke_with_result('mapDataset', map_dataset_info)
        print (chunk_dict)
        global_dict=mergeDict(global_dict, chunk_dict, lambda n1,n2: n1+n2)
        # llamar a funcion
        chunk_start=chunk_end+1
        
    print (global_dict)
    print ("End of mapping")
 
def show_help ():
    print (" MapReduce program. Needs a chunk size (optional) and a dataset name (compulsive).")
    
def mergeDict (map_result1, map_result2, mergingFunct= lambda x,y:x):
    map_result1['word_counting']+=map_result2['word_counting']
    for k,v in dict(map_result2['word_count']).items():
        if k in map_result1['word_count']:
            map_result1['word_count'][k] = mergingFunct(map_result1['word_count'][k], v)
        else:
            map_result1['word_count'][k] = v
    return map_result1
       
if __name__ == "__main__":
    main() 
    

    

    
    
    


