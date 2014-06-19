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

        # TEST ALL THE THINGS! 

        output_list.append( Field_activity_type(d).value ) 
        output_list.append( Field_verb(d).value ) 
        output_list.append( Field_id(d).value ) 
        output_list.append( Field_objecttype(d).value ) 
        output_list.append( str(Field_object(d).value) ) 
        output_list.append( Field_postedtime(d).value )  
        output_list.append( Field_body(d).value )  
        output_list.append( Field_link(d).value )  
        output_list.append( Field_twitter_lang(d).value )  
        output_list.append( Field_favoritescount(d).value )  
        output_list.append( Field_retweetcount(d).value )  
        output_list.append( Field_twitter_filter_level(d).value )  
        output_list.append( Field_inreplyto_link(d).value )  
        output_list.append( Field_provider_objecttype(d).value )  
        output_list.append( Field_provider_displayname(d).value )  
        output_list.append( Field_provider_link(d).value )  
        output_list.append( Field_generator_displayname(d).value )  
        output_list.append( Field_generator_link(d).value )  
        output_list.append( str(Field_gnip_rules(d).value) )  
        output_list.append( str(Field_gnip_urls(d).value) )  
        output_list.append( Field_gnip_language_value(d).value )  
        output_list.append( Field_gnip_klout_score(d).value )  
        output_list.append( str(Field_gnip_klout_profile_topics(d).value) )  
        output_list.append( str(Field_gnip_klout_profile_klout_user_id(d).value) )  
        output_list.append( str(Field_gnip_klout_profile_link(d).value) )  
        output_list.append( Field_gnip_profilelocations_displayname(d).value )
        output_list.append( Field_gnip_profilelocations_objecttype(d).value )
        output_list.append( Field_gnip_profilelocations_geo_type(d).value )
        output_list.append( str(Field_gnip_profilelocations_geo_coordinates(d).value) )
        output_list.append( Field_gnip_profilelocations_address_country(d).value )
        output_list.append( Field_gnip_profilelocations_address_countrycode(d).value )
        output_list.append( Field_gnip_profilelocations_address_locality(d).value )
        output_list.append( Field_gnip_profilelocations_address_region(d).value )
        output_list.append( Field_gnip_profilelocations_address_subregion(d).value )
        output_list.append( Field_actor_id(d).value )  
        output_list.append( Field_actor_objecttype(d).value )  
        output_list.append( Field_actor_postedtime(d).value )  
        output_list.append( Field_actor_displayname(d).value )  
        output_list.append( Field_actor_preferredusername(d).value )  
        output_list.append( Field_actor_summary(d).value )  
        output_list.append( Field_actor_link(d).value )  
        output_list.append( Field_actor_image(d).value )  
        output_list.append( Field_actor_language(d).value )  
        output_list.append( str(Field_actor_links(d).value) )  
        output_list.append( Field_actor_twittertimezone(d).value )  
        output_list.append( Field_actor_utcoffset(d).value )  
        output_list.append( Field_actor_verified(d).value )  
        output_list.append( Field_actor_location_displayname(d).value )  
        output_list.append( Field_actor_location_objecttype(d).value )  
        output_list.append( Field_actor_followerscount(d).value )  
        output_list.append( Field_actor_friendscount(d).value )  
        output_list.append( Field_actor_listedcount(d).value )  
        output_list.append( Field_actor_statusesCount(d).value )  
        output_list.append( Field_actor_favoritesCount(d).value )  
        output_list.append( str(Field_twitter_entities_urls(d).value) )  
        output_list.append( str(Field_twitter_entities_hashtags(d).value) )  
        output_list.append( str(Field_twitter_entities_symbols(d).value) )  
        output_list.append( str(Field_twitter_entities_user_mentions(d).value) )  
        output_list.append( str(Field_twitter_entities_media(d).value) )  
        output_list.append( Field_geo_type(d).value )  
        output_list.append( str(Field_geo_coordinates(d).value) )  
        output_list.append( Field_location_displayname(d).value )  
        output_list.append( Field_location_name(d).value )  
        output_list.append( Field_location_objecttype(d).value )  
        output_list.append( Field_location_twitter_country_code(d).value )  
        output_list.append( Field_location_country_code(d).value )  
        output_list.append( Field_location_link(d).value )  
        output_list.append( Field_location_geo_type(d).value )  
        output_list.append( str(Field_location_geo_coordinates(d).value) )  

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

