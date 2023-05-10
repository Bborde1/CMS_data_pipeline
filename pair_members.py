#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This is a function for pairing an even group of people
"""


import pandas as pd
import numpy as np

def pair_members(group=['Denise', 'Brandon', 'Talia', 'Jori', 'Isaac', 'Hanh']):
    local_group=group #Fix scoping idiot
    pairings=[]
    
    #This assumes an even group size, can enforce later
    assert len(group) % 2 == 0, "Group size is not even, add a member or empty string"
    
    while len(local_group) > 2:
        new_pair = []
        
        new_pair.append(local_group.pop(0))
        
        list_length_index = len(local_group)-1
        
        random_member = np.random.randint(0, list_length_index)
        
        new_pair.append(local_group.pop(random_member))
        
        pairings.append(new_pair)
    
    pairings.append(local_group)
    
    return pairings
