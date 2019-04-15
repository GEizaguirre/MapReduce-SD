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
       
    dataset_size=658
    dataset_end=dataset_size
    ds_char='bytes='+str(dataset_end)+'-'+str(dataset_end)
    
    while (COS_session.get_object(bucket_name,
                                dataset_name,
                                extra_get_args={'Range': ds_char})).decode('utf-8').isalnum():
        dataset_end+=1
        ds_char='bytes='+str(dataset_end)+'-'+str(dataset_end)
        
    print ("Last position: ", dataset_end-1)
    ds_range='bytes='+str(0)+'-'+str(dataset_end-1)
    
    map_dataset_info={'cos_config':{'endpoint':configuration_paramethers.cos_config['endpoint'],
                      'secret_key':configuration_paramethers.cos_config['secret_key'],
                      'access_key':configuration_paramethers.cos_config['access_key']},
                      'bucket_name':bucket_name, 'dataset_name':dataset_name,
                      'ds_range':ds_range}

    #print (map_dataset.map_dataset(map_dataset_info))
    
    fn_session=ibm_cf_connection.CloudFunctions(functions_config)
    configure_functions.configure_fn(fn_session)
    #paramets={'name':'Juan'}
    #print(fn_session.invoke("helloPython", paramets))
    print(fn_session.invoke_with_result('mapDataset', map_dataset_info))
 
def show_help ():
    print (" MapReduce program. Needs a chunk size (optional) and a dataset name (compulsive).")
       
if __name__ == "__main__":
    main() 
    

    

    
    
    


