'''
Created on 13 mar. 2019

@author: German
'''
import sys
from configuration_paramethers import bucket_name
from cos_backend import COSbackend
import ibm_botocore
import re

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
    
    session=COSbackend()
    dataset_list = []
    try:
        # Create list of datasets.
        for elem in session.list_objects(bucket_name):
            dataset_list.append(elem['Key'])  
        # Consult if the chosen dataset is available.
        if dataset_name in dataset_list:
            dataset_size = session.head_object(bucket_name, dataset_name)['content-length']
            print (" Chosen ", dataset_name, " with size ", dataset_size, "B. ")
        else:
            print (" Dataset ", dataset_name, " could not be found in bucket ", bucket_name)
        print ( " Processing... ")
    except ibm_botocore.exceptions.ClientError:
        print ( " Bucket ", bucket_name, " not found.")
        
    dataset_size=1024
    map_dataset (0, dataset_size, session, dataset_name)
  
def map_dataset (low_limit, high_limit, current_session, ds_name):
    ds_range='bytes='+str(low_limit)+'-'+str(high_limit)
    ds_text=current_session.get_object(bucket_name, ds_name,  extra_get_args={'Range': ds_range})
    ds_dict=dict()
    ds_count=0
    ds_list=re.sub("[^\w\s\-]", " ",  ds_text.decode('utf-8')).split()
    for word in ds_list:
        ds_count += 1
        if word in ds_dict:
            ds_dict[word] += 1
        else: ds_dict[word] = 1
    print (" Number of words: ", ds_count)
    print (" WordCount: ", ds_dict)
 
def show_help ():
    print (" MapReduce program. Needs a chunk size (optional) and a dataset name (compulsive).")
       
if __name__ == "__main__":
    main() 
    

    

    
    
    


