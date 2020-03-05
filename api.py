import json
from sodapy import Socrata


def print_firstnrecords(API_KEY: str, page_size: int = 1000, num_pages: int = 1, output: str = "results.json"):

	dataset = 'nc67-uf89'
	datasource = "data.cityofnewyork.us"


	try:
		client = Socrata(datasource, API_KEY)

		if not num_pages:

			#num_pages = the number of rows /  page_size

			# run a query to count the number of rows in the dataset

			count_response = client.get(dataset, select = 'COUNT(*)')
			count = int(count_response[0].get('COUNT'))


			num_pages = round(count/page_size)
			page = 0

			while page < num_pages:

				if page == 0:
					results = client.get(dataset, limit = page_size)
				else:
					results += client.get(dataset, limit = page_size, offset = page * page_size)
				page += 1

		else:
			#total number = page_size * num_pages
			limit = page_size * num_pages
			results = client.get(dataset, limit = limit)

		if not output: 
			#no output file specified. print formatted json
			json_formatted_str = json.dumps(results, indent = 2)
			print(json_formatted_str) 

		else:
			#write into output file. If it doesn't exist, create it first. 
			with open(output, 'w') as outfile:
				json.dump(results, outfile)

			print(f"Successfully wrote {len(results)} JSON data into file: {output}")

		#return results
	except Exception as e:
			print(f"Something went wrong: {e}")
			raise
