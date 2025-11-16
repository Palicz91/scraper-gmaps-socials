scraper has 3 parts
1- make queries
2- search query
3- get place data



1- make queries

we will place 
	all locations in locations.txt
	all brands in brands.txt
	all categories in categories.txt

and run python make_queries.py
it will save all queries to google_maps_queries.txt


2- search query

Now we will run python search_query.py
it will read all queries from google_maps_queries.txt and search them one by one on google maps
in return it will save all places links in links.txt


3- get place data
Now we will run python get_place_data.py
it will open all links that are saved in links.txt and scrape each location data and save to csv file 