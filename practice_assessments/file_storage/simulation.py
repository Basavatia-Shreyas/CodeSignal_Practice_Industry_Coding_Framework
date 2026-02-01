import json
import math
import string
import re
import random
import sys
import traceback
import functools
from collections import OrderedDict
from datetime import datetime, timedelta

import numpy
import sortedcontainers

def simulate_coding_framework(list_of_lists):
    """
    Simulates a coding framework operation on a list of lists of strings.

    Parameters:
    list_of_lists (List[List[str]]): A list of lists containing strings.
    """
    us = UploadServer()
    res = list()
    for sublist in list_of_lists:
        operation = sublist[0]
        args = sublist[1:]

        match operation:
            case "FILE_UPLOAD":
                res.append(us.file_upload(*args))
            case "FILE_GET":
                res.append(us.file_get(*args))
            case "FILE_COPY":
                res.append(us.file_copy(*args))
            case "FILE_SEARCH":
                res.append(us.file_search(*args))
            case "FILE_UPLOAD_AT":
                res.append(us.file_upload_at(*args))
            case "FILE_GET_AT":
                res.append(us.file_get_at(*args))
            case "FILE_COPY_AT":
                res.append(us.file_copy_at(*args))
            case "FILE_SEARCH_AT":
                res.append(us.file_search_at(*args))
            case "ROLLBACK":
                res.append(us.rollback(*args))

    return res

class UploadServer:
    def __init__(self):
        self.files = {}

    def file_upload(self, file_name, size):
        if file_name in self.files.keys():
            raise RuntimeError("File already exists")
        
        self.files[file_name] = size

        return "uploaded " + file_name

    def file_get(self, filename):
        try:
            file_size = self.files[filename]
            return "got " + filename
        except:
            return None
        
    def file_copy(self, source, dest):
        try:
            source_size = self.files[source]
            self.files[dest] = source_size
            return "copied " + source + " to " + dest
        except:
            raise RuntimeError("Source file doesn't exist")
    
    def file_search(self, prefix):
        matches = [(f, self.files[f]) for f in self.files if f.startswith(prefix)]

        matches.sort(key=lambda x: (-int(x[1][:-2]), x[0]))

        return_string = "found ["
        for i in matches[:10]:
            return_string += i[0] + ", "

        return_string = return_string[:-2] + "]"

        return return_string
    
    def file_upload_at(self, timestamp, file_name, file_size, ttl = None):
        if file_name in self.files.keys():
            raise RuntimeError("File already exists")

        if ttl:
            self.files[file_name] = (file_size, datetime.fromisoformat(timestamp), ttl)
        else:
            self.files[file_name] = (file_size, datetime.fromisoformat(timestamp))

        return "uploaded at " + file_name

    def file_get_at(self, timestamp, file_name):
        try:
            stamp = datetime.fromisoformat(timestamp)
            file_info = self.files[file_name]
            # testing
            if len(file_info) == 3:
                temp = file_info[1] + timedelta(seconds=file_info[2])
                bool_val = temp < stamp
            
            if len(file_info) == 3 and file_info[1] + timedelta(seconds=file_info[2]) < stamp:
                return "file not found"

            return "got at " + file_name
        except:
            return "file not found"
        
    def file_copy_at(self, timestamp, file_from, file_to):
        try:
            stamp = datetime.fromisoformat(timestamp)
            source_file = self.files[file_from]
            if len(source_file) == 3 and source_file[1] + timedelta(seconds=source_file[2]) < stamp:
                raise RuntimeError("Source file doesn't exist")

            if len(source_file) == 3:
                self.files[file_to] = (source_file[0], stamp, source_file[2])
            else:
                self.files[file_to] = (source_file[0], stamp)

            return "copied at " + file_from + " to " + file_to
        except:
            raise RuntimeError("Source file doesn't exist")
        
    def file_search_at(self, timestamp, prefix):
        stamp = datetime.fromisoformat(timestamp)
        matches = [(key, value) for key, value in self.files.items() if key.startswith(prefix) and (len(value) == 2 or (len(value) == 3 and value[1] + timedelta(seconds=value[2]) >= stamp))]

        matches.sort(key=lambda x: (-int(x[1][0][:-2]), x[0]))

        return_string = "found at ["
        for i in matches[:10]:
            return_string += i[0] + ", "

        return_string = return_string[:-2] + "]"

        return return_string
    
    def rollback(self, timestamp):
        # Remove entries that were added after the timestamp
        # If we copied ovewriting a previous file that existed before the rollback but not after this might be an issue
        stamp = datetime.fromisoformat(timestamp)
        new_dict = {k: v for k, v in self.files.items() if v[1] < stamp}
        self.files = new_dict
        return "rollback to " + timestamp