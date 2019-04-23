'''
Created on 22 mar. 2019

Module with the class ResultLog for managing the results from
the mapReduce application.

@author: German Telmo Eizaguirre Suarez
@contact: germantelmoeizaguirre@estudiants.urv.cat
@organization: Universitat Rovira i Virgili

'''

class ResultLog ():
    
    def __init__(self, cos_session):
        self.dict = dict()
        self.dict['counting_words'] = 0
        self.dict['word_count'] = dict()
        self.received_maps=0
        self.sent_maps=0
        self.COS_session = cos_session
        
    def increaseSent (self):
        self.sent_maps+=1
        
    def increaseReceived (self):
        self.received_maps+=1
        
    def reduceEnd (self):
        return self.received_maps == self.sent_maps