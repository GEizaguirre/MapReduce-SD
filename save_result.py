'''
Created on 16 abr. 2019

@author: German
'''

class ResultLog ():
    
    def __init__(self, cos_session):
        self.dict = dict()
        self.dict['word_count'] = dict()
        self.dict['counting_words'] = 0
        self.received_maps=0
        self.sent_maps=0
        self.COS_session = cos_session
        
    def increaseSent (self):
        self.sent_maps+=1
        
    def increaseReceived (self):
        self.received_maps+=1
        
    def reduceEnd (self):
        return self.received_maps == self.sent_maps