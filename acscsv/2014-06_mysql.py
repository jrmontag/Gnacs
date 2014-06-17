#!/usr/bin/env python
# -*- coding: UTF-8 -*-
__author__="Scott Hendrickson, Josh Montague"
__license__="Simplified BSD"

import sys
from datetime import datetime
import re
import os
import codecs
import json
import acscsv

# for custom twitter output, import both the fields module and the code module
from twitter_acs_fields import *
import twitter_acs


#
# define any custom field extractor classes here. inherit from acscsv.* as needed 
#
class Field_twitter_hashtags_text_DB(acscsv._LimitedField):
    """
    Combine the first 'limit' hashtags found in the payload into a list and assign to 
    self.value. If there are less than 'limit', the list is padded with 
    self.default_value 
    """
    path = ["twitter_entities", "hashtags"]
    fields = ["text"]

    def __init__(self, json_record, limit=5):
        super(
            Field_twitter_hashtags_text_DB
            , self).__init__(json_record, limit=limit) 


class Field_twitter_symbols_text_DB(acscsv._LimitedField):
    """
    Assign to self.value a list of twitter_entities.symbols 'text's.
    This one is a little experimental... haven't seen it in the wild (Activity Streams) yet, 
    so using https://blog.twitter.com/2013/symbols-entities-tweets as the template.
    """
    path = ["twitter_entities", "symbols"]
    fields = ["text"]

    def __init__(self, json_record, limit=5):
        super(
            Field_twitter_symbols_text_DB
            , self).__init__(json_record, limit=limit)


class Field_twitter_mentions_id_name_DB(acscsv._LimitedField):
    """
    Assign to self.value a list of 'limit' twitter_entities.user_mentions.screen_name and .id pairs 
    (in order, but in a flat list).
    """
    path = ["twitter_entities", "user_mentions"]
    fields = ["id", "screen_name"]

    def __init__(self, json_record, limit=5):
        super(
            Field_twitter_mentions_id_name_DB
            , self).__init__(json_record, limit=limit)


class Field_twitter_urls_tco_expanded_DB(acscsv._LimitedField):
    """
    combination of two classes above
    """
    path = ["twitter_entities", "urls"]
    fields = ["url", "expanded_url"] 

    def __init__(self, json_record, limit=5):
        super(
            Field_twitter_urls_tco_expanded_DB 
            , self).__init__(json_record, limit=limit)


class Field_twitter_media_id_url_DB(acscsv._LimitedField):
    """
    Assign to self.value a list of twitter_entities.media.id and .expanded_url pairs
    """
    path = ["twitter_entities", "media"]
    fields = ["id", "expanded_url"]

    def __init__(self, json_record, limit=5):
        super(
            Field_twitter_media_id_url_DB 
            , self).__init__(json_record, limit=limit)


class Field_actor_links(acscsv._Field):
    """
    Assign to self.value a string repr of a list of the links contained in actor.links (or 
    default_value if the corresponding dict is empty). The number of links included in the 
    list is optionally specified in the constructor. 
    """
    path = ["actor", "links"]

    def __init__(self, json_record, limit=2):
        super(
            Field_actor_links
            , self).__init__(json_record)
        if self.value != self.default_value:
            # ignore the links that are "null" in the payload ( ==> None in the dict )
            self.value_list = [ x["href"] for x in self.value if x["href"] is not None ] 
            self.value_list = self.fix_length( self.value_list, limit )




#
# define your main subclass here (eg inherit from twitter_acs.TwacsCSV)
#
class CustomCSV( twitter_acs.TwacsCSV ):
    """
    Customized output experiment: 2014-05-05 (JM)

    Goal is for the parsed JSON to end up in separate, delimited files suitable for loading 
    into a relational database (MySQL to start). 

 
    Test class for experimenting with new output combinations. This class should inherit
    from the appropriate module.class in the core library. Compliance and invalid records 
    are handled by the parent class' procRecord() method. This class should only define a 
    new get_output_list() method which overrides the parent method and determines the 
    custom output. 
    """       
    split_flag = "GNIPSPLIT"


    def combine_lists(self, *args):
        """
        Takes an arbitrary number of lists to be combined into a single string for passing back 
        to main gnacs.py and subsequent splitting. The special delimiter for parsing the string 
        back into separate lists is defined in here.
        """
        #flag = "GNIPSPLIT"

        # append flag to the end of all but last list
        [ x.append(self.split_flag) for i,x in enumerate(args) if i < (len(args) - 1) ]
        # combine the lists into one
        combined_list = []
        [ combined_list.extend(x) for x in args ] 

        return combined_list


    def get_output_list(self, d):
        """
        Take a JSON Activity Streams payload as a Python dictionary.

        Custom output format designed for loading multiple database tables. Creates N separate 
        lists of activity fields, joins them on a GNIPSPLIT to be split at the gnacs.py level 
        and written to separate files.      
        """
        # timestamp format
        t_fmt = "%Y-%m-%d %H:%M:%S"
        now = datetime.utcnow().strftime( t_fmt )

        default_value = acscsv._Field({}).default_value

        # hash_list will be [ id, tag1, id, tag2, ... ] and will be split in gnacs.py
        hash_list = [] 
        for i in Field_twitter_hashtags_text_DB(d).value_list:
            if i != default_value:      # temporary hack to exclude all the "NULL" values
                hash_list.extend( [ Field_id(d).value, i ] ) 

        acs_list = [
                    Field_id(d).value 
                    , Field_gnip_rules(d).value 
                    , now 
                    , Field_postedtime(d).value 
                    , Field_verb(d).value 
                    , Field_actor_id(d).value 
                    , Field_body(d).value 
                    , Field_twitter_lang(d).value 
                    , Field_gnip_language_value(d).value 
                    , Field_link(d).value 
                    , Field_generator_displayname(d).value 
                    ] \
                    + Field_geo_coordinates(d).value_list \
                    + Field_twitter_symbols_text_DB(d).value_list \
                    + Field_twitter_mentions_id_name_DB(d).value_list \
                    + Field_twitter_urls_tco_expanded_DB(d).value_list \
                    + Field_twitter_media_id_url_DB(d).value_list 

        ustatic_list = [
                    now 
                    , Field_actor_id(d).value 
                    , Field_id(d).value         # needs to be updated programatically in an actual app 
                    , Field_actor_postedtime(d).value 
                    , Field_actor_preferredusername(d).value 
                    , Field_actor_displayname(d).value 
                    , Field_actor_link(d).value 
                    , Field_actor_summary(d).value 
                    ] \
                    + Field_actor_links(d, limit=2).value_list \
                    + [ 
                    Field_actor_twittertimezone(d).value 
                    , Field_actor_utcoffset(d).value 
                    , Field_actor_verified(d).value 
                    , Field_actor_language(d).value 
                    ] \
                    + Field_gnip_profilelocations_geo_coordinates(d).value_list \
                    + [ 
                    Field_gnip_profilelocations_address_countrycode(d).value
                    , Field_gnip_profilelocations_address_locality(d).value
                    , Field_gnip_profilelocations_address_region(d).value
                    , Field_gnip_profilelocations_address_subregion(d).value
                    , Field_gnip_profilelocations_displayname(d).value
                    , Field_gnip_klout_profile_klout_user_id(d).value
                    ]

        udyn_list = [ 
                    Field_actor_id(d).value 
                    , Field_id(d).value         # needs to be updated programatically in an actual app 
                    , Field_gnip_klout_score(d).value 
                    ] \
                    + Field_gnip_klout_profile_topics(d).value_list \
                    + [
                    Field_actor_statusesCount(d).value 
                    , Field_actor_followerscount(d).value  
                    , Field_actor_friendscount(d).value  
                    , Field_actor_listedcount(d).value  
                    ]

        output_list = self.combine_lists(
                                        acs_list
                                        , ustatic_list
                                        , udyn_list
                                        , hash_list 
                                        )

        return output_list 


#TODO: possible future extension of ^
#        # this is the acs_list if all twitter entities are stripped out into separate tables
#        acs_list = [
#                    Field_id(d).value 
#                    , Field_gnip_rules(d).value 
#                    , now 
#                    , Field_postedtime(d).value 
#                    , Field_verb(d).value 
#                    , Field_actor_id(d).value 
#                    , Field_body(d).value 
#                    , Field_twitter_lang(d).value 
#                    , Field_gnip_lang(d).value 
#                    , Field_link(d).value 
#                    , Field_generator_displayname(d).value 
#                    ] \
#                    + Field_geo_coords(d).value_list \
#
#
#        #
#        sym_list = []
#        for i in Field_twitter_symbols_text_DB(d).value_list:
#            sym_list.extend( [ Field_id(d).value, i ] ) 
#        #
#        # nb: this isn't going to work yet. need to split out the actId, uid, uname triplets
#        mentions_list = []
#        for i in Field_twitter_mentions_id_name_DB(d).value_list: 
#            mentions_list.extend( [ Field_id(d).value, i ] ) 
#        #
#        urls_list = []
#        media_list = []



    def _multi_file_DB_1(self, d):
        """
        !! 
        This is an early version of the output_list method... superceded by the one above.
            This will definitely crash if you try to run it without updating various names 
                and classes
        !!

        Custom output format designed for loading multiple database tables.
        Creates 3 separate lists of activity fields, joins them on a GNIPSPLIT to be split 
        at the gnacs.py level and written to separate files.      
    
        Superceded 2014-05-12 for more star-schema approach of e.g. hashtag/mention/link/... tables
        (JM) 
        """
        # timestamp format
        t_fmt = "%Y-%m-%d %H:%M:%S"
        now = datetime.utcnow().strftime( t_fmt )

        # the explicit .value attr reference is needed
        acs_list = [
                    Field_id(d).value 
                    , Field_gnip_rules(d).value 
                    , now 
                    , Field_postedtime(d).value 
                    , Field_verb(d).value 
                    , Field_actor_id(d).value 
                    , Field_body(d).value 
                    , Field_twitter_lang(d).value 
                    , Field_gnip_lang(d).value 
                    , Field_link(d).value 
                    , Field_generator_displayname(d).value 
                    ] \
                    + Field_geo_coords(d).value_list \
                    + Field_twitter_hashtags_text_DB(d).value_list \
                    + Field_twitter_symbols_text_DB(d).value_list \
                    + Field_twitter_mentions_id_name_DB(d).value_list \
                    + Field_twitter_urls_tco_expanded_DB(d).value_list \
                    + Field_twitter_media_id_url_DB(d).value_list 

        ustatic_list = [
                    now 
                    , Field_actor_id(d).value 
                    , Field_id(d).value         # needs to be updated programatically in an actual app 
                    , Field_actor_postedtime(d).value 
                    , Field_actor_preferredusername(d).value 
                    , Field_actor_displayname(d).value 
                    , Field_actor_acct_link(d).value 
                    , Field_actor_summary(d).value 
                    ] \
                    + Field_actor_links(d, limit=2).value_list \
                    + [ 
                    Field_actor_twittertimezone(d).value 
                    , Field_actor_utcoffset_DB(d).value 
                    , Field_actor_verified(d).value 
                    , Field_actor_lang(d).value 
                    ] \
                    + Field_gnip_pl_geo_coords(d).value_list \
                    + [ 
                    Field_gnip_pl_countrycode(d).value
                    , Field_gnip_pl_locality(d).value
                    , Field_gnip_pl_region(d).value
                    , Field_gnip_pl_subregion(d).value
                    , Field_gnip_pl_displayname(d).value
                    , Field_gnip_klout_user_id(d).value
                    ]

        udyn_list = [ 
                    Field_actor_id(d).value 
                    , Field_id(d).value         # needs to be updated programatically in an actual app 
                    , Field_gnip_klout_score(d).value 
                    ] \
                    + Field_gnip_klout_topics(d).value_list \
                    + [
                    Field_actor_statuses_DB(d).value 
                    , Field_actor_followers_DB(d).value  
                    , Field_actor_friends_DB(d).value  
                    , Field_actor_listed_DB(d).value  
                    ]
        #
        #TODO: replace with combine_lists() method above
        #
        # consider instead sending the combined list and an arbitrary list of positions to split it
        #flag = "GNIPSPLIT"  # this is hardcoded into gnacs.py, as well. change both if needed!
        # only need mortar on first two bricks 
        [ x.append(self.split_flag) for x in acs_list, ustatic_list ]
        combined_list = [] 
        combined_list.extend(acs_list)
        combined_list.extend(ustatic_list)
        combined_list.extend(udyn_list)
        #
        return combined_list 



if __name__ == "__main__":
    """
    Receive data from stdin (decompressed JSON-lines only) and process using the existing 
    code, but let this module's get_output_list() method define the output fields. 
    """

    # Get the appropriate object by mocking the constructor in the main gnacs.py code. most 
    #   common command-line options (flags) don't matter since we're explicitly defining the 
    #   fields to be printed in the method above 
    #   ex: TwacsCSV("|", None, *[True]*7) 
    processing_obj = CustomCSV("|", None, *[True]*7) 

    # set up files and dirs for output
    #data_dir = os.environ['HOME'] + "/gnacs_db"
    data_dir = os.environ["PWD"] + "/gnacs_db"
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    # open file objects for writing 
    acs_f = codecs.open( data_dir + '/table_activities.csv', 'wb', 'utf8') 
    ustatic_f = codecs.open( data_dir + '/table_users_static.csv', 'wb', 'utf8') 
    udyn_f = codecs.open( data_dir + '/table_users_dynamic.csv', 'wb', 'utf8') 
    hash_f = codecs.open( data_dir + '/table_hashtags.csv', 'wb', 'utf8') 

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
            #   
            # TODO: swap procRecord call for get_source_list(), split accordingly, clean 
            #           up next 30 loc
            # record is parsed and returned as a single list, split and trimmed of delimiters
            compRE = re.compile(r"GNIPREMOVE") 
            # choose the emptyField so it will convert to MySQL NULL
            tmp_combined_rec = processing_obj.procRecord(record, emptyField="\\N")
   
            if compRE.search(tmp_combined_rec): 
                sys.stderr.write("Skipping compliance activity: ({}) {}\n"
                        .format(line_number, tmp_combined_rec) ) 
                continue
            # otherwise, write to appropriate file objects (from above)
            #flag = "GNIPSPLIT"      
            acs_str, ustatic_str, udyn_str, hash_str = tmp_combined_rec.split( processing_obj.split_flag ) 
            # clean up any leading/trailing pipes 
            acs_str = acs_str.strip("|")
            ustatic_str = ustatic_str.strip("|")
            udyn_str = udyn_str.strip("|")
            hash_str = hash_str.strip("|")                    # id|tag1|id|tag2|...
            hash_list = re.findall("[^|]+\|[^|]+", hash_str)  # [ 'id|tag1', 'id|tag2', ... ] 
            #   
            acs_f.write(acs_str + "\n")
            ustatic_f.write(ustatic_str + "\n")
            udyn_f.write(udyn_str + "\n")
            [ hash_f.write(x + "\n") for x in hash_list ] 


