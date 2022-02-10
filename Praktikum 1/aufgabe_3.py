#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 14 21:24:47 2021

@author: wolfedgar
"""

import matplotlib.pyplot as plt
from itertools import groupby
import datetime

def plot_airplane_visibily_in_time_context(filename):
    plt.figure(figsize=(8,6))
    plane_data = []
    
    with open(filename) as f:
        for line in f:
            line_values = line.split(",")
            plane_id = line_values[0]
            date = line_values[1]
            time = line_values[2]
            
            date_time = date + " " + time
            
            datetime_object = datetime.datetime.strptime(date_time, 
                                                          "%Y/%m/%d %H:%M:%S.%f")
            
            datetime_object = datetime_object.replace(microsecond=0, second=0, minute=0)
            
            plane_data.append((datetime_object, plane_id))
            
    plane_data.sort()

    
    plane_ids_already_seen = []
    plane_date_counts_dict = {}
    
    for plane_data_value in plane_data:
        date = plane_data_value[0]
        plane_id = plane_data_value[1]
        
        if date not in plane_date_counts_dict:
            plane_date_counts_dict[date] = 1
            plane_ids_already_seen.clear()
            plane_ids_already_seen.append(plane_id)
        else:
            # Neues Flugzeug
            if plane_id not in plane_ids_already_seen:
                plane_date_counts_dict[date] += 1
                plane_ids_already_seen.append(plane_id)
    
                

    
    xv = plane_date_counts_dict.keys()
    yv = plane_date_counts_dict.values()
    
    plt.scatter(xv, yv)
    plt.plot(xv, yv)
    
        
        
        


    #plt.plot(date_objects, counts)
   # plt.scatter(date_objects, counts)
    #plt.show()
    
if __name__ == '__main__':
    plot_airplane_visibily_in_time_context("/data/adsb/adsbprak2.txt")
    
    
