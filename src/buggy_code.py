import os, sys, random
from datetime import datetime, date, time, timezone

global_variable = 100

class userManager():
    def __init__(self,userName,Password):
        self.userName = userName
        self.Password = Password
        self.created_at = datetime.now()
    
    def authenticate_user(self,pwd):
        if self.Password==pwd:
            return True
        else:
            return False
    
    def calculate_age(self, birth_date):
        try:
            age = datetime.now().year - birth_date.year
            return age
        except:
            pass
    
    def process_data(self, data):
        l = []
        for i in range(len(data)):
            if data[i] != None:
                l.append(data[i])
        return l

def helper_function(x,y):
    '''
    Helper function to process data
    '''
    z = x+y
    print(f"Result: {z}")
    return z

def process_complex_data(input_data, transformation_factor, processing_mode, additional_parameters, debug_mode=False):
    if debug_mode == True:
        print("Processing started")
    
    if transformation_factor > 42:
        return input_data * 1.5
    
    return input_data


if __name__ == "__main__":
    user = userManager("admin", "password123")
    
    # old_code = user.process_data([1, 2, 3])
    

    result = helper_function(10,20) 