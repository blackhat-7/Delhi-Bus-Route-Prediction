import schedule
from google.transit import gtfs_realtime_pb2
import urllib.request
import pandas as pd
import time

store = []
def process_data(data):
	for i,entity in enumerate(data):
		bus_id = entity.id
		route_id = entity.vehicle.trip.route_id
		lat = entity.vehicle.position.latitude
		lon = entity.vehicle.position.longitude
		timestamp = entity.vehicle.timestamp
		store.append([bus_id,lat,lon,route_id,timestamp])
	return

def fetch_data():
	print("Fetching data...")
	feed = gtfs_realtime_pb2.FeedMessage()
	response = urllib.request.urlopen('https://opendata.iiitd.edu.in/api/realtime/VehiclePositions.pb?key=5kOKha1VJqxWvkkIp1J71Yrb5mc3VsDS')
	feed.ParseFromString(response.read())
	process_data(feed.entity)
	return 

def add_to_df():
	print("adding data...")
	global store
	df = pd.DataFrame(store,columns=['BusId','Latitude','Longitude','RouteId','Timestamp'])
	store=[]
	name= time.strftime("%m%d%H%M")
	with open(name +".csv", 'w') as f:
		df.to_csv(f)
	return

def start():
	print("Started all schedules")
	schedule.every(10).seconds.do(fetch_data).tag("fetcher")
	schedule.every(1).minutes.do(add_to_df).tag("adder")
	print("schedule added")
	return

def stop():
	schedule.clear("fetcher")
	add_to_df()
	schedule.clear("adder")
	print("all schedules cleared")
	return

schedule.every().day.at("05:00").do(start)
schedule.every().day.at("23:30").do(stop)
while True:
	schedule.run_pending()