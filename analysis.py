import logging
from logging.handlers import QueueHandler, QueueListener
from EvictQueue import EvictQueue
import pandas as pd
from geopy.distance import geodesic
from datetime import datetime, timedelta, time
import numpy as np
import os

# Setup logger root
rootLogger = logging.getLogger()
rootLogger.setLevel(logging.INFO)  # Set the minimum log level

# Timestamp for the filename
current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_filename = f"logs_{current_time}.log"  # e.g., logs_2023-01-01_12-00-00.log

# Ensure a directory exists for the logs
if not os.path.exists('logs'):
    os.makedirs('logs')

# Full path for the log file
full_log_path = os.path.join('logs', log_filename)

# Create handlers for console and file output
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)
fileHandler = logging.FileHandler(full_log_path)
fileHandler.setLevel(logging.INFO)

# Common formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
consoleHandler.setFormatter(formatter)
fileHandler.setFormatter(formatter)

# Create an EvictQueue and QueueHandler
log_que = EvictQueue(1000)
queue_handler = QueueHandler(log_que)

# Set up the QueueListener with both handlers
queue_listener = QueueListener(log_que, consoleHandler, fileHandler)
queue_listener.start()

# Clear existing handlers and add only the QueueHandler
rootLogger.handlers = []  
rootLogger.addHandler(queue_handler)

# Setup done
logging.info("Logger setup complete with file logging")


# Load data
df_routes = pd.read_csv('https://raw.githubusercontent.com/maxdotr/UTA-Punctuality-Analysis/main/routes.csv')
df_stop_times = pd.read_csv('https://raw.githubusercontent.com/maxdotr/UTA-Punctuality-Analysis/main/stop_times.csv')
df_stops = pd.read_csv('https://raw.githubusercontent.com/maxdotr/UTA-Punctuality-Analysis/main/stops.csv')
df_trips = pd.read_csv('https://raw.githubusercontent.com/maxdotr/UTA-Punctuality-Analysis/main/trips.csv')
bus_locations_df = pd.read_csv('https://raw.githubusercontent.com/maxdotr/UTA-Punctuality-Analysis/main/bustimes-small-sample.csv', low_memory=False)

# Adjust the time handling function - data between df's is different
def clean_and_convert_time(time_str):
    time_str = time_str.strip().replace('24:', '00:')
    dt = pd.to_datetime(time_str, format='%H:%M:%S', errors='coerce')
    if pd.isna(dt):
        return None  # Skip or handle invalid times
    return dt.time()


# Apply time cleaning function
logging.info("Start cleaning times")
bus_locations_df['time'] = bus_locations_df['time'].apply(clean_and_convert_time)
df_stop_times['arrival_time'] = df_stop_times['arrival_time'].apply(clean_and_convert_time)
logging.info("Stop cleanign times")

logging.info("Starting merges")
# Merge operations
df_trips_routes = pd.merge(df_trips, df_routes[['route_id', 'route_short_name']], on='route_id', how='left')
logging.info(df_trips_routes)
df_trip_route_stoptimes = pd.merge(df_trips_routes, df_stop_times[['trip_id', 'arrival_time', 'stop_id']], on='trip_id', how='left')
logging.info(df_trip_route_stoptimes)
final_df = pd.merge(df_trip_route_stoptimes, df_stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']], on='stop_id', how='left')
logging.info(final_df)

# Select relevant columns and sort
expected_route_punctuality_df = final_df[['stop_id', 'route_short_name', 'stop_name', 'trip_headsign', 'stop_lat', 'stop_lon', 'arrival_time']]
expected_route_punctuality_df = expected_route_punctuality_df.sort_values(by=['stop_id', 'arrival_time'])
logging.info(expected_route_punctuality_df)
logging.info("Merges end")

# Calculates distance based on coords
def calculate_distance(lat1, lon1, lat2, lon2):
    return geodesic((lat1, lon1), (lat2, lon2)).meters

# Find closest approach
def find_closest_approach(bus_df, stop):
    bus_df['distance_to_stop'] = bus_df.apply(
        lambda row: calculate_distance(row['latitude'], row['longitude'], stop['stop_lat'], stop['stop_lon']), axis=1)
    min_distance_idx = bus_df['distance_to_stop'].idxmin()
    return bus_df.loc[min_distance_idx]

def parse_date(date_str):
    try:
        # Attempt to parse the date assuming 'dd/mm/yy'
        return datetime.strptime(date_str, "%d/%m/%y").date()
    except ValueError:
        logging.error("Date format error for date: %s", date_str)
        return None 

# Calculates interval for calculations
def calculate_operational_intervals(scheduled_times):
    arbitrary_date = datetime(2000, 1, 1)
    datetime_list = [datetime.combine(arbitrary_date, t) for t in scheduled_times if isinstance(t, time)]

    # Assuming operational hours are from 6 AM to 10 PM
    operational_start = datetime.combine(arbitrary_date, time(6, 0))
    operational_end = datetime.combine(arbitrary_date, time(21, 0))

    # Filter datetimes to include only those within operational hours
    operational_times = [dt for dt in datetime_list if operational_start <= dt <= operational_end]

    # Calculate intervals in minutes between consecutive times
    intervals = [(operational_times[i] - operational_times[i-1]).total_seconds() / 60 for i in range(1, len(operational_times))]
    logging.debug("Stop interval: %s", np.median(intervals))
    if intervals:
        if(np.median(intervals) >= 30.0):
            return 30.0 
        else:
            return 15.0
    else:
        return 15 

def match_times(bus_df, stop_info, scheduled_times):
    matched_times = []
    unique_dates = bus_df['date'].dropna().unique()

    logging.debug("Unique dates found in bus locations data: %s", unique_dates)

    average_interval = calculate_operational_intervals(scheduled_times)
    half_interval = average_interval / 2

    for unique_date in unique_dates:
        current_date = parse_date(unique_date)
        if current_date is None:
            continue  # Skip processing if date parsing failed

        daily_bus_df = bus_df[bus_df['date'] == unique_date]
        used_indices = set()  # Set to track indices of matched bus entries

        logging.debug("Processing matches for date: %s", current_date)

        # Remove any NaT values and sort scheduled_times in ascending order
        valid_times = [t for t in scheduled_times if t is not pd.NaT]
        scheduled_times_sorted = sorted(valid_times)

        for scheduled_time in scheduled_times_sorted:
            if isinstance(scheduled_time, time):
                full_datetime = datetime.combine(current_date, scheduled_time)
                time_window_start = (full_datetime - timedelta(minutes=half_interval))
                time_window_end = (full_datetime + timedelta(minutes=half_interval))

                # Adjusting time window to not start before 6 AM or end after 9 PM
                earliest_start = datetime.combine(current_date, time(6, 0))
                latest_end = datetime.combine(current_date, time(21, 0))

                if time_window_start < earliest_start:
                    time_window_start = earliest_start
                if time_window_end > latest_end:
                    time_window_end = latest_end

                # Convert back to time objects
                time_window_start = time_window_start.time()
                time_window_end = time_window_end.time()

                temp_df = daily_bus_df[
                    (daily_bus_df['time'] >= time_window_start) &
                    (daily_bus_df['time'] <= time_window_end) &
                    (daily_bus_df['routeNum'] == stop_info['route_short_name']) &
                    (~daily_bus_df.index.isin(used_indices))
                ].copy()

                if not temp_df.empty:
                    closest_bus = find_closest_approach(temp_df, stop_info)
                    matched_time = {
                        'vehicle_id': closest_bus['vehicleID'],
                        'stop_id': stop_info['stop_id'],
                        'stop_name': stop_info['stop_name'],
                        'route': closest_bus['routeNum'],
                        'destination': closest_bus['destination'],
                        'scheduled_arrival_time': scheduled_time,
                        'actual_arrival_time': closest_bus['time'],
                        'distance_to_stop': closest_bus['distance_to_stop'],
                        'match_date': current_date
                    }
                    matched_times.append(matched_time)
                    used_indices.add(closest_bus.name)

                    logging.info("Match Found! %s", matched_time)
                else:
                    logging.debug("No buses found within the time window for scheduled time: %s on route %s for date %s",
                                 scheduled_time, stop_info['route_short_name'], current_date)
            else:
                logging.info("Invalid time object encountered: %s", scheduled_time)

    return matched_times






# Convert times in expected_route_punctuality_df
expected_route_punctuality_df['arrival_time'] = pd.to_datetime(expected_route_punctuality_df['arrival_time'], format='%H:%M:%S').dt.time

# Prepare bus locations data
bus_locations_df['routeNum'] = bus_locations_df['routeNum'].astype(str)
bus_locations_df.sort_values(['routeNum', 'destination', 'time'], inplace=True)

# Group by stop to handle multiple arrivals and perform the matching
all_results = []
grouped = expected_route_punctuality_df.groupby(['stop_id', 'route_short_name', 'trip_headsign'])

logging.info("Expected route punctuality:  %s", expected_route_punctuality_df)

# Group by stop to handle multiple arrivals and perform the matching
all_results = []
grouped = expected_route_punctuality_df.groupby(['stop_id', 'route_short_name', 'trip_headsign'])

for name, group in grouped:
    stop_info = {
        'stop_id': name[0],
        'stop_name': group['stop_name'].iloc[0],
        'route_short_name': name[1],
        'trip_headsign': name[2],
        'stop_lat': group.iloc[0]['stop_lat'],
        'stop_lon': group.iloc[0]['stop_lon']
    }
    # Convert scheduled times to a set and back to list to remove duplicates
    scheduled_times = list(set(group['arrival_time'].tolist()))
    
    results = match_times(bus_locations_df, stop_info, scheduled_times)
    all_results.extend(results)

# Convert all results to a DataFrame for analysis
final_results_df = pd.DataFrame(all_results)


# Check results
print(final_results_df.head())


# Assuming final_results_df['actual_arrival_time'] and final_results_df['scheduled_arrival_time'] are in datetime.time format
def compute_time_difference(row):
    # Choose an arbitrary date for converting time to datetime
    arbitrary_date = datetime(2000, 1, 1)  # Y2K why not?!
    # Combine time with arbitrary date
    actual_datetime = datetime.combine(arbitrary_date, row['actual_arrival_time'])
    scheduled_datetime = datetime.combine(arbitrary_date, row['scheduled_arrival_time'])
    
    # Calculate time difference
    time_difference = actual_datetime - scheduled_datetime
    return time_difference.total_seconds() / 60

# Apply this function to calculate time differences
final_results_df['time_difference'] = final_results_df.apply(compute_time_difference, axis=1)

# Print discrepancies over a threshold (e.g., over 5 minutes)
print(final_results_df)

# Generate timestamped file name
timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
file_name = f"final_results_{timestamp}.csv"

# Save to CSV
final_results_df.to_csv(file_name, index=False)