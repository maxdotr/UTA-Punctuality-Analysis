import pandas as pd
from geopy.distance import geodesic
from datetime import datetime, date, time, timedelta
import logging

log_que = queue.Queue(-1)
queue_handler = logging.handlers.QueueHandler(log_que)
log_handler = logging.StreamHandler()
queue_listener = logging.handlers.QueueListener(log_que, log_handler)
queue_listener.start()
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s  %(message)s", handlers=[queue_handler])

# Load data
df_routes = pd.read_csv('https://raw.githubusercontent.com/maxdotr/UTA-Punctuality-Analysis/main/routes.csv')
df_stop_times = pd.read_csv('https://raw.githubusercontent.com/maxdotr/UTA-Punctuality-Analysis/main/stop_times.csv')
df_stops = pd.read_csv('https://raw.githubusercontent.com/maxdotr/UTA-Punctuality-Analysis/main/stops.csv')
df_trips = pd.read_csv('https://raw.githubusercontent.com/maxdotr/UTA-Punctuality-Analysis/main/trips.csv')
bus_locations_df = pd.read_csv('https://raw.githubusercontent.com/maxdotr/UTA-Punctuality-Analysis/main/bustimes-small-sample.csv', low_memory=False)

# Adjust the time handling function
def clean_and_convert_time(time_str):
    # Handling '24:xx:xx' format by converting '24' to '00'
    time_str = time_str.strip().replace('24:', '00:')
    dt = pd.to_datetime(time_str, format='%H:%M:%S', errors='coerce')
    if pd.isna(dt):
        return None  # Skip or handle invalid times
    return dt.time()


# Apply time cleaning function
bus_locations_df['time'] = bus_locations_df['time'].apply(clean_and_convert_time)
df_stop_times['arrival_time'] = df_stop_times['arrival_time'].apply(clean_and_convert_time)

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

# Define a function to calculate distance
def calculate_distance(lat1, lon1, lat2, lon2):
    return geodesic((lat1, lon1), (lat2, lon2)).meters

# Find closest approach
def find_closest_approach(bus_df, stop):
    bus_df['distance_to_stop'] = bus_df.apply(
        lambda row: calculate_distance(row['latitude'], row['longitude'], stop['stop_lat'], stop['stop_lon']), axis=1)
    min_distance_idx = bus_df['distance_to_stop'].idxmin()
    return bus_df.loc[min_distance_idx]

# Match actual times with scheduled times
from datetime import datetime, date, time, timedelta

from datetime import datetime, date, timedelta
import logging

def match_times(bus_df, stop_info, scheduled_times):
    matched_times = []
    unique_dates = bus_df['date'].dropna().unique()  # Assuming 'date' column is in YYYY-MM-DD format

    # Log the unique dates found
    logging.info("Unique dates found in bus locations data: %s", unique_dates)

    for unique_date in unique_dates:
        # Convert string date to datetime.date object
        current_date = datetime.strptime(unique_date, "%Y-%m-%d").date()

        # Filter buses for the current date
        daily_bus_df = bus_df[bus_df['date'] == unique_date]

        logging.info("Processing matches for date: %s", current_date)

        for scheduled_time in scheduled_times:
            # Check if scheduled_time is a valid datetime.time object
            if isinstance(scheduled_time, time):
                # Create a datetime from the time object and the current date
                full_datetime = datetime.combine(current_date, scheduled_time)

                # Create time windows for comparison
                time_window_start = (full_datetime - timedelta(minutes=30)).time()
                time_window_end = (full_datetime + timedelta(minutes=30)).time()

                # Log the time window being used for matching
                logging.info("Evaluating window: %s to %s for scheduled time: %s on route %s for date %s", 
                             time_window_start, time_window_end, scheduled_time, stop_info['route_short_name'], current_date)

                # Filter buses within the time window and by route number
                temp_df = daily_bus_df[(daily_bus_df['time'] >= time_window_start) & (daily_bus_df['time'] <= time_window_end) & 
                                       (daily_bus_df['routeNum'] == stop_info['route_short_name'])].copy()

                if not temp_df.empty:
                    closest_bus = find_closest_approach(temp_df, stop_info)
                    matched_time = {
                        'vehicle_id': closest_bus['vehicleID'],
                        'stop_id': stop_info['stop_id'],
                        'route': closest_bus['routeNum'],
                        'destination': closest_bus['destination'],
                        'scheduled_arrival_time': scheduled_time,
                        'actual_arrival_time': closest_bus['time'],
                        'distance_to_stop': closest_bus['distance_to_stop'],
                        'match_date': current_date  # Include the date of the match
                    }
                    matched_times.append(matched_time)

                    # Log the match found
                    logging.info("Match found: %s", matched_time)
                else:
                    # Log no buses found within the window for this route on this date
                    logging.info("No buses found within the time window for scheduled time: %s on route %s for date %s", scheduled_time, stop_info['route_short_name'], current_date)
            else:
                # Log invalid time object encountered
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

for name, group in grouped:
    stop_info = {
        'stop_id': name[0],
        'route_short_name': name[1],
        'trip_headsign': name[2],
        'stop_lat': group.iloc[0]['stop_lat'],
        'stop_lon': group.iloc[0]['stop_lon']
    }
    scheduled_times = group['arrival_time'].tolist()
    results = match_times(bus_locations_df, stop_info, scheduled_times)
    all_results.extend(results)

# Convert all results to a DataFrame for analysis
final_results_df = pd.DataFrame(all_results)

# Check results
print(final_results_df.head())

# Analyze discrepancies in actual and scheduled times
final_results_df['time_difference'] = final_results_df.apply(
    lambda row: (pd.Timestamp(row['actual_arrival_time']) - pd.Timestamp(row['scheduled_arrival_time'])).seconds / 60, axis=1)

# Print discrepancies over a threshold (e.g., over 5 minutes)
print(final_results_df[final_results_df['time_difference'] > 5])
