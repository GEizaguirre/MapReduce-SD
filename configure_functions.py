'''
Created on 13 mar. 2019

@author: German
'''

''' This function will upload the necessary actions to IBM cloud '''

import requests

def configure_fn (fn_session):
    
    #try:
    act_info=fn_session.get_action("mapDataset")
    if "error" in act_info:
        if act_info["error"]=="The requested resource does not exist.": print ("Action mapDataset does not exist.")
        else: print ("Error at mapDataset invocation")
        print ("A function mapDataset will be loaded to IBM functions from map_dataset.py")
    else: print ("Remote action mapDataset exits and will be executed.")
        
    '''
    except requests.exceptions.MissingSchema:
        print (" La accion no existe.")
        # Descomprimir?
        fn_file = open('map_dataset.py', 'r')
        fn_code = fn_file.read()
        fn_session.create_action("mapDataset", fn_code)
    '''