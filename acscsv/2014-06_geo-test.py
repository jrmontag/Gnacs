#!/usr/bin/env python
# -*- coding: UTF-8 -*-
__author__="Josh Montague"
__license__="Simplified BSD"

import sys
import json
import acscsv

# for custom twitter output, import both the fields module and the code module
import twitter_acs
from twitter_acs_fields import *


#
# define any custom field extractor classes here. inherit from acscsv.* as needed 
#



class CustomCSV(twitter_acs.TwacsCSV):
    """ 
    Test class for experimenting with new output combinations. This class should inherit
    from the appropriate module.class in the core library. Compliance and invalid records 
    are handled by the parent class' procRecord() method. This class should only define a 
    new get_output_list() method which overrides the parent method and determines the 
    custom output. 
    """  

    def get_output_list(self, d):
        """
        2014-06-11 (JM) - working on a super sweet new custom output for a specific use-case! 
            Need: timestamps, all geo-tag-related fields, and nothing else. 

        This method overwrites the output method in the parent class. Append values from 
        desired payload fields to the output list which is returned at the end of this method. 
        
        Take a JSON Activity Streams payload as a Python dictionary. 
        """
        output_list = [] 

        # postedTime
        output_list.append( Field_postedtime(d).value )

        # geo fields
        output_list.append( Field_geo_type(d).value )
        output_list.append( str(Field_geo_coordinates(d).value) )   # list => str

        # location fields 
        output_list.append( Field_location_displayname(d).value )
        output_list.append( Field_location_name(d).value )
        output_list.append( Field_location_objecttype(d).value )
        output_list.append( Field_location_twitter_country_code(d).value )
        output_list.append( Field_location_country_code(d).value )
        output_list.append( Field_location_link(d).value )
        output_list.append( Field_location_geo_type(d).value )
        output_list.append( Field_location_geo_coordinates(d).value )


        # done building output list 
        return output_list 



if __name__ == "__main__":
    """
    Receive data from stdin (decompressed JSON-lines only) and process using the existing 
    code, but let this module's get_output_list() method define the output fields. 
    """

    # Get the appropriate object by mocking the constructor in the main gnacs.py code. most 
    #   common command-line options (flags) don't matter since we're explicitly defining the 
    #   fields to be printed in the method above 
    processing_obj = CustomCSV("|", None, *[True]*7) 

    for line_number, record in processing_obj.file_reader(): 
        # note: this doesn't handle broken pipe errors  
        sys.stdout.write("%s\n"%processing_obj.procRecord(record, emptyField="None"))

