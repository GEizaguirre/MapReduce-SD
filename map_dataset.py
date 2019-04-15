'''
Created on 9 abr. 2019

@author: German
'''
import re
from cos_backend import COSbackend

def map_dataset (map_dataset_info):
    
    COS_session=COSbackend(map_dataset_info['cos_config'])
    ds_chunk=COS_session.get_object(map_dataset_info['bucket_name'],
                                    map_dataset_info['dataset_name'],
                                    extra_get_args={'Range': map_dataset_info['ds_range']})
    
    ds_chunk=re.sub("[^\w\s\-]", " ",  ds_chunk.decode('utf-8'))
    ds_dict=dict()
    ds_count=0
    ds_list=ds_chunk.lower().split()
    for word in ds_list:
        ds_count += 1
        if word in ds_dict:
            ds_dict[word] += 1
        else: ds_dict[word] = 1
        
    ds_dict_sorted = sorted(ds_dict.items(), key=lambda x: x[1], reverse=True)
    
    return ({'word_counting':ds_count, 'word_count':ds_dict_sorted, 'text':ds_chunk})


    
