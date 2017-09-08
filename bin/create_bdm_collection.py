'''
  The intended purpose of this program is to join fields across one or more documents into a single new document.
  That new document will represent a BDM collection.
'''
import copy
import inspect
import socket
import getpass
import multiprocessing
import sys
import os
import csv
import glob
from pymongo import MongoClient
from datetime import timezone, datetime

username = getpass.getuser()

hostname = socket.gethostname()
ip_address = socket.gethostbyname(hostname)

study_code = 'CP220'
session_key = '1504804224.565506'
bdm_collection_name = 'Some BDM Collection Name'

host = 'localhost'
port = '27017'
uri = 'mongodb://' + host + ':' + port

dt = datetime.utcnow()

print("Date now is '%s'"  % dt)

executable = os.path.abspath(__file__)

current_session_id = dt.replace(tzinfo=timezone.utc).timestamp()

verbose = True

LIMIT_FILE_COUNT = True

MAX_FILE_COUNT = 10

LIMIT_ROW_COUNT = True

MAX_ROW_COUNT = 10

db_name = 'bdmdb'

source_collection_name = 'files'
target_collection_name = 'bdm_collections'

collection_map_rules = []

## function definitions will be stringified here
cleaned_collection_map_rules = []

desired_fields = ['DOMAIN', 'SAFFL', 'RACE', 'AGELO', 'AGEU']

mongoclient = MongoClient(uri)

db = mongoclient[db_name]

source_collection = db[source_collection_name]

metadata = {}

def age_transform(age):

	age = int(age)

	if age < 10:
		age -= 1
	elif age == 10:
		age += 4
	elif age > 20:
		age +=30

	return age


def prepare_metadata():

	global metadata

	metadata = {
		'username' : username,
		'hostname' : hostname,
		'ip_address' : ip_address,
		'collection_map_rules' : cleaned_collection_map_rules,				
		'session_id' : current_session_id, 
		'study_code' : study_code, 
		'session_date' : dt,
		'executable' :  executable,
		'source_session_id' : session_key}


def get_cleaned_rule(rule):

	if 'transform' in rule:
		for key in rule['transform']:
			content = inspect.getsourcelines(rule['transform'][key])
			rule['transform'][key] = content

	return rule


def load_map_rules():

	global collection_map_rules

	rule1 = {
		'source_path' : '/home/sundaramj/BDM_SRC_DATA/CP220/tssp0.csv',
		'study_code' : study_code,
		'target_collection' : target_collection_name,
		'join_key' : 'USUBJID',
		'map' : {
			'DOMAIN' : 'tssp0_DOMAIN',
			'SAFFL' : 'tssp0_SAFFL',
			'RACE' : 'tssp0_ETHNICITY',
			'USUBJID' : 'subject_id'
			}
	}

	collection_map_rules.append(rule1)

	cleaned_rule1 = copy.deepcopy(rule1)
	cleaned_rule1 = get_cleaned_rule(cleaned_rule1)
	cleaned_collection_map_rules.append(cleaned_rule1)

	rule2 = {
		'source_path' : '/home/sundaramj/BDM_SRC_DATA/CP220/ie0.csv',
		'study_code' : study_code,
		'target_collection' : target_collection_name,
		'join_key' : 'USUBJID',
		'map' : {
			'AGE' : 'ie0_AGE',
			'AGEU' : 'ie0_AGE UNITS',
			# },
			},
		'transform' : {
			'AGE' : age_transform
		}
	}


	collection_map_rules.append(rule2)

	cleaned_rule2 = copy.deepcopy(rule2)
	cleaned_rule2 = get_cleaned_rule(cleaned_rule2)
	cleaned_collection_map_rules.append(cleaned_rule2)
	

def get_document_lookup(rule):

	projection = {}

	join_key = rule['join_key']
	
	projection[join_key] = 1

	projection['_id'] = 0

	for mapkey in rule['map']:
		projection[mapkey] = 1

	documents = source_collection.find({'etl_metadata.study_code' : rule['study_code'], 'etl_metadata.source_path' : rule['source_path']}, projection)

	document_lookup = {}

	doc_ctr = 0

	for doc in documents:
		
		doc_key = doc[join_key]

		if verbose:
			print("Found join_key '%s'" % doc_key)

		document_lookup[doc_key] = doc

		doc_ctr += 1


	if verbose:
		print("Found '%d' documents" % doc_ctr)
		# print(document_lookup)

	return document_lookup


def create_bdm_collection():

	rule1 = collection_map_rules[0]
	rule2 = collection_map_rules[1]

	document_lookup1 = get_document_lookup(rule1)
	document_lookup2 = get_document_lookup(rule2)

	target_document_list = []

	doc_ctr = 0

	for doc_key in document_lookup1:
	
		if verbose:
			print("Processing key '%s'" % doc_key)

		target_doc = {}

		doc1 = document_lookup1[doc_key]
	
		for source_field in rule1['map']:
			target_field = rule1['map'][source_field]
			value = doc1[source_field]
			if 'transform' in rule1:
				if source_field in rule1['transform']:
					value = rule1['transform'][source_field](value)
						# value = rule1['transform'][source_field][transform_rule_key](value)
						# transform_function = rule1['transform'][source_field][transform_rule_key]
						# value = transform_function(value)
			target_doc[target_field] = value
	
		if doc_key in document_lookup2:

			doc2 = document_lookup2[doc_key]

			for source_field in rule2['map']:
				target_field = rule2['map'][source_field]

				value = doc2[source_field]
				if 'transform' in rule2:
					if source_field in rule2['transform']:
						print("value '%s' source_field '%s'" % (value, source_field))
						value = rule2['transform'][source_field](value)
							# transform_function = rule2['transform'][source_field][transform_rule_key]
							# value = rule2['transform'][source_field][transform_rule_key](value)
							# value = transform_function(value)
				target_doc[target_field] = value
		else:
			print("Could not find corresponding key '%s' in document2"  % doc_key)

		doc_ctr += 1

		target_doc['etl_metadata'] = metadata

		target_document_list.append(target_doc)

		if verbose:
			print(target_doc)
		
	print("Will attempt to insert %d documents into the collection '%s'" % (doc_ctr, target_collection_name))


	mongoclient = MongoClient(uri)

	db = mongoclient[db_name]

	target_collection = db[target_collection_name]

	for target_doc in target_document_list:
		try:
			target_collection.insert(target_doc)
			# target_collection.insert_many(target_document_list)
		except:
			print("Unexpected error:", sys.exc_info()[0])



if __name__ == '__main__':

	load_map_rules()

	prepare_metadata()

	create_bdm_collection()