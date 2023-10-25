import os
import requests
import zipfile
import pandas as pd
from datetime import datetime, timedelta
import pyproj

target_directory = "../data"

start_date = datetime.strptime('2023-08-01', '%Y-%m-%d')
end_date = datetime.strptime('2023-08-31', '%Y-%m-%d')
                             
current_date = start_date

while current_date <= end_date:
    
    # Convert the current date to the desired format for the URL
    date_str = current_date.strftime('%Y-%m-%d')
    # Define the URL of the ZIP file and the target directory
    print('Downloading data for date: ', date_str)
    url = f"http://web.ais.dk/aisdata/aisdk-{date_str}.zip"


    # Create the target directory if it doesn't exist
    os.makedirs(target_directory, exist_ok=True)

    # Define the path where the ZIP file will be saved
    zip_file_path = os.path.join(target_directory, f'aisdk-{date_str}.zip')

    # Download the ZIP file
    response = requests.get(url)

    # Check if the download was successful
    if response.status_code == 200:
        with open(zip_file_path, "wb") as file:
            file.write(response.content)
        print(f"Downloaded successfully {url}.")
    else:
        print(f"Failed to download {url}. Status code: {response.status_code}")
        exit()

    # Unzip the downloaded file
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(target_directory)
        print(f"Unzipped successfully {zip_file_path}.")

    # Clean up: Remove the downloaded ZIP file
    os.remove(zip_file_path)
    print(f"Cleaned up the ZIP file {zip_file_path}.")

    current_date += timedelta(days=1)

def filter_geographical_area(chunk, min_lat, max_lat, min_lon, max_lon):
    return chunk[
        (chunk['Latitude'] >= min_lat) & (chunk['Latitude'] <= max_lat) &
        (chunk['Longitude'] >= min_lon) & (chunk['Longitude'] <= max_lon)
    ]



def calculate_bounding_box_utm(center_lat, center_lon, distance_km):
    # Create a UTM projection centered around the specified point
    utm_zone = int((center_lon + 180) / 6) + 1
    utm_proj = pyproj.Proj(proj='utm', zone=utm_zone, ellps='WGS84')

    # Project the center point to UTM coordinates
    center_x, center_y = utm_proj(center_lon, center_lat)

    # Calculate bounding box in UTM coordinates
    min_x = center_x - distance_km * 1000
    max_x = center_x + distance_km * 1000
    min_y = center_y - distance_km * 1000
    max_y = center_y + distance_km * 1000

    # Convert back to latitude and longitude
    min_lon, min_lat = utm_proj(min_x, min_y, inverse=True)
    max_lon, max_lat = utm_proj(max_x, max_y, inverse=True)

    return min_lat, max_lat, min_lon, max_lon


def make_geographical_area_df(file_path,lat,lon, radius,chunk_size=10000):

    csv_reader = pd.read_csv(file_path, chunksize=chunk_size, iterator=True)

    df = pd.DataFrame()
    # Calculate bounding box coordinates for a 10 km radius
    min_lat, max_lat, min_lon, max_lon = calculate_bounding_box_utm(lat, lon, radius)
    for chunk in csv_reader:
        filtered_chunk = filter_geographical_area(chunk, min_lat, max_lat, min_lon, max_lon)
        df = pd.concat([df, filtered_chunk], ignore_index=True)
    return df


current_date = start_date


oresund_center_lat = 55.6517
oresund_center_lon = 12.9372
oresund_radius_km = 16
oresund_loc = (oresund_center_lat, oresund_center_lon, oresund_radius_km)


aarhus_port_lat = 56.1532
aarhus_port_lon = 10.2044
aarhus_port_radius_km = 10
aarhus_port_loc = (aarhus_port_lat, aarhus_port_lon, aarhus_port_radius_km)

northern_tip_jutland_lat = 57.7486
northern_tip_jutland_lon = 10.2039
northern_tip_jutland_radius_km = 20
northern_tip_jutland_loc = (northern_tip_jutland_lat, northern_tip_jutland_lon, northern_tip_jutland_radius_km)

locs = {'oresund' : oresund_loc,'aarhus' : aarhus_port_loc, 'north_jutland' :northern_tip_jutland_loc}

def custom_mode(series):
    """Returns the most frequent value, excluding NaNs."""
    frecs = series.mode()
    if len(frecs) > 0 :
        ship_type = frecs[0]
        if ship_type == 'Undefined' and len(frecs) > 1:
            ship_type = frecs[1]
    else:
        ship_type = np.nan
    return ship_type

agg_functions = {
    'Latitude': 'mean',
    'Longitude': 'mean',
    'Type of mobile': custom_mode,
    'Heading': 'mean',
    'Navigational status': custom_mode,
    'ROT': 'mean',
    'SOG': 'mean',
    'COG': 'mean',
    'IMO': 'first',
    'Ship type': custom_mode,
    'Length': 'first',
    'Width': 'first',
    'Draught': 'mean',
    'Destination': custom_mode,
    'Cargo type': custom_mode,
    'Data source type': 'first',
    'A': 'first',
    'B': 'first',
    'C': 'first',
    'D': 'first',
    'Type of position fixing device': 'first',
    'Callsign' : 'first',
    'Name' : 'first',
    'ETA' : 'first'
}

while current_date <= end_date:
    # Proccessing the dowloaded files

    date_str = current_date.strftime('%Y-%m-%d')
    file_path = os.path.join(target_directory, f'aisdk-{date_str}.csv')

    for loc_name ,(lat, lon, rad) in locs.items():

        df = make_geographical_area_df(file_path,lat,lon,rad)

        df['# Timestamp'] = pd.to_datetime(df['# Timestamp'])
        df = df.groupby(['MMSI', pd.Grouper(key='# Timestamp', freq='1min')]).agg(agg_functions).reset_index()

        loc_csv_path = os.path.join(target_directory, f'{loc_name}-{date_str}.csv')
        df.to_csv(loc_csv_path ,mode = 'a', index=False)
        


    print(f"Processed successfully {date_str}.")


    current_date += timedelta(days=1)
