'''
Created on 13 mar. 2019

@author: German
'''
import sys

''' Default chunk size '''
chunk_size=1

def main ():
    if (len(sys.argv)>1):
        chunk_size=sys.argv[1]
        print(" Chunk size was set to ", chunk_size, "MB")
    else:   
        print(" Chunk size was set to the default value of ", chunk_size, "MB")
        
    # TODO: get object by paramether, get size, calulate range.
    
    
    
    
if __name__ == "__main__":
    main() 
    
    
    
    


