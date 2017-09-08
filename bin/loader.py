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

host = 'localhost'
port = '27017'
uri = 'mongodb://' + host + ':' + port

dt = datetime.utcnow()

print("Date now is '%s'"  % dt)

executable = os.path.abspath(__file__)

session_id = dt.replace(tzinfo=timezone.utc).timestamp()

verbose = False

LIMIT_FILE_COUNT = False

MAX_FILE_COUNT = 10

LIMIT_ROW_COUNT = False

MAX_ROW_COUNT = 10

db_name = 'bdmdb'

collection_name = 'files'

PATH = '/home/sundaramj/BDM_SRC_DATA'

file_list = [file for file in glob.glob(PATH + '/**/*.csv', recursive=True)]



def process_csv_file(file):

	if verbose:
		print("Processing file '%s'" % file)


	dirname = os.path.dirname(file)
	study_code = os.path.basename(dirname)

	reader = csv.reader(open(file))
	
	line_ctr = 0
	
	column_position_to_name_lookup = {}
	
	record_list = []

	for row in reader:

		if line_ctr == 0:
			
			field_ctr = 0
			
			for field in row:
			
				column_position_to_name_lookup[field_ctr] = field
			
				field_ctr += 1			
		else:

			if LIMIT_ROW_COUNT:
				if line_ctr == MAX_ROW_COUNT:
					break

			doc = {}

			metadata = {
				'hostname' : hostname,
				'ip_address' : ip_address,
				'username' : username,
				'source_path' : file, 
				'session_id' : session_id, 
				'study_code' : study_code, 
				'session_date' : dt,
				'executable' :  executable}

			field_ctr = 0

			for field in row:
			
				field_name = column_position_to_name_lookup[field_ctr]
			
				doc[field_name] = field
			
				field_ctr += 1

			if verbose:
				print(doc)

			doc['etl_metadata'] = metadata
			# sys.exit(1)
			
			record_list.append(doc)

		line_ctr += 1

	print("Will attempt to insert %d documents into the collection '%s'" % (line_ctr, collection_name))


	mongoclient = MongoClient(uri)

	db = mongoclient[db_name]

	collection = db[collection_name]

	try:
		collection.insert_many(record_list)
	except:
		print("Unexpected error:", sys.exc_info()[0])



if __name__ == '__main__':

	file_count = len(file_list)

	if verbose:
		print("Found %d .csv files in directory '%s'"  % (file_count, PATH))
		print("Will process them now")

	pool = multiprocessing.Pool()

	pool.map(process_csv_file, file_list)

	if verbose:
		print("Done.  Please check the collection '%s' MongoDB database '%s'")