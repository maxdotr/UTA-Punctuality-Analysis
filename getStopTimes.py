import requests
import schedule
from datetime import datetime
from datetime import time
import pytz
import csv
import os 
import time

# Filename and field names setup
filename = "bustimes.csv"
fields = ['vehicleID', 'routeNum', 'destination', 'latitude', 'longitude', 'bearing', 'weekday', 'date', 'time']
headersWritten = False

# Open the file once, write the header, and then append data rows
# Check and write headers if necessary
def writeHeadersIfNeeded():
    # Check if file exists and has content; only write headers if it's new or empty
    if not os.path.isfile(filename) or os.stat(filename).st_size == 0:
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            writer.writeheader()

# Get routes that pass through downtown Salt Lake and University, not counting limited routes
routes = [1, 17, 2, 200, 205, 209, 21, 213, 220, 223, 4, 455, 470, 509, 9]

# Track the buses and log their times
def trackRoutes():
    if shouldTrack():
        for route in routes:

            # Fetching data from the API
            requestURL = f"https://webapi.rideuta.com/api/VehicleLocation/{route}"
            request = requests.get(requestURL)
            requestJSON = request.json()

            # Append data to the CSV file without overwriting it
            with open(filename, 'a', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fields)
                for itemJSON in requestJSON:
                    MTC = pytz.timezone('America/Denver')
                    dateTimeMTC = datetime.now(MTC)

                    bus_data = {
                        'vehicleID': itemJSON['vehicleId'],
                        'routeNum': itemJSON['routeNum'],
                        'destination': itemJSON['destination'],
                        'latitude': itemJSON['location']['latitude'],
                        'longitude': itemJSON['location']['longitude'],
                        'bearing': itemJSON['bearing'],
                        'weekday': dateTimeMTC.weekday(),
                        'date': dateTimeMTC.strftime('%x'),
                        'time': dateTimeMTC.strftime('%X')
                    }
                    writer.writerow(bus_data)

def shouldTrack():
    MTC = pytz.timezone('America/Denver')
    now_time = datetime.now(MTC).time()
    start_time = datetime.strptime('06:00:00', '%H:%M:%S').time()
    end_time = datetime.strptime('21:00:00', '%H:%M:%S').time()
    return start_time <= now_time <= end_time


schedule.every(1).minutes.do(trackRoutes)
# Main loop
if __name__ == "__main__":
    writeHeadersIfNeeded()  # Ensure headers are written if needed on start/restart
    while True:
        schedule.run_pending()
        time.sleep(5)  # Sleep for a short time to prevent a busy-wait loop