#!/usr/bin/env python
# -*- coding: UTF-8 -*-
__author__="Josh Montague"
__license__="Simplified BSD"

import sys
import json
import acscsv

# for custom twitter output, import both the fields module and the code module
from twitter_acs_fields import *
import twitter_acs


#
# define any custom field extractor classes here. inherit from acscsv.* as needed 
#



class CustomCSV( twitter_acs.TwacsCSV ):
    """
    Test class for experimenting with new output combinations. This class should inherit
    from the appropriate module.class in the core library. Compliance and invalid records 
    are handled by the parent class' procRecord() method. This class should only define a 
    new get_output_list() method which overrides the parent method and determines the 
    custom output. 
    """       
    
    def get_output_list(self, d):
        """
        Use this method to overwrite the output method in the parent class. Append values from 
        desired payload fields to the output list, which is returned at the end of this method. 
        
        Take a JSON Activity Streams payload as a Python dictionary. 
        """
        output_list = [] 

        # test all the things 

        #print >>sys.stderr, "*** object={}".format(Field_object(d).value)

        #output_list.append( Field_activity_type(d).value ) 
        #output_list.append( Field_verb(d).value ) 
        #output_list.append( Field_id(d).value ) 
        #output_list.append( Field_objecttype(d).value ) 
        #output_list.append( str(Field_object(d).value) ) 
        #output_list.append( Field_postedtime(d).value )  
        #output_list.append( Field_body(d).value )  
        #output_list.append( Field_link(d).value )  
        #output_list.append( Field_twitter_lang(d).value )  
        #output_list.append( Field_favoritescount(d).value )  
        #output_list.append( Field_retweetcount(d).value )  
        #output_list.append( Field_twitter_filter_level(d).value )  
        #output_list.append( Field_inreplyto_link(d).value )  
        #output_list.append( Field_provider_objecttype(d).value )  
        #output_list.append( Field_provider_displayname(d).value )  
        #output_list.append( Field_provider_link(d).value )  
        #output_list.append( Field_generator_displayname(d).value )  
        #output_list.append( Field_generator_link(d).value )  
        #output_list.append( str(Field_gnip_rules(d).value) )  
        output_list.append( str(Field_gnip_urls(d).value) )  



#        Field_gnip_rules
#        Field_gnip_urls
#        Field_gnip_language_value
#        Field_gnip_klout_score
#        Field_gnip_klout_profile_topics
#        Field_gnip_klout_profile_klout_user_id
#        Field_gnip_klout_profile_link
#        Field_actor_id
#        Field_actor_objecttype
#        Field_actor_postedtime
#        Field_actor_displayname
#        Field_actor_preferredusername
#        Field_actor_summary
#        Field_actor_link
#        Field_actor_image
#        Field_actor_language
#        Field_actor_links
#        Field_actor_twittertimezone
#        Field_actor_utcoffset
#        Field_actor_verified
#        Field_actor_location_displayname
#        Field_actor_location_objecttype
#        Field_actor_followerscount
#        Field_actor_friendscount
#        Field_actor_listedcount
#        Field_actor_statusesCount
#        Field_actor_favoritesCount
#        Field_twitter_entities_urls
#        Field_twitter_entities_hashtags
#        Field_twitter_entities_symbols
#        Field_twitter_entities_user_mentions
#        Field_twitter_entities_media
#        Field_geo_type
#        Field_geo_coordinates
#        Field_location_displayname
#        Field_location_name
#        Field_location_objecttype
#        Field_location_twitter_country_code
#        Field_location_country_code
#        Field_location_link
#        Field_location_geo_type
#        Field_location_geo_coordinates



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

    line_number = 0 
    for r in sys.stdin: 
        line_number += 1
        try:
            recs = [json.loads(r.strip())]
        except ValueError:
            try:
                # maybe a missing line feed?
                recs = [json.loads(x) for x in r.strip().replace("}{", "}GNIP_SPLIT{").split("GNIP_SPLIT")]
            except ValueError:
                sys.stderr.write("Invalid JSON record (%d) %s, skipping\n"%(line_number, r.strip()))
                continue
        for record in recs:
            if len(record) == 0:
                continue
            sys.stdout.write("%s\n"%processing_obj.procRecord(record, emptyField="None"))

