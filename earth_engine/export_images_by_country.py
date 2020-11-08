import argparse
import ee
import json
import os
import pandas as pd
import time
from tqdm import tqdm
import urllib as ur
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

PROJECT = 'omdena-wri'
SERVICE_ACCOUNT_STR ='sa1-311@omdena-wri.iam.gserviceaccount.com'
KEY = 'private-key.json'
BUCKET = '1182020'

def authenticate(project, service_account_str, key):
    # # GCP instructions
    # # 1. Install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/quickstarts)

    # https://colab.research.google.com/github/google/earthengine-api/blob/master/python/examples/ipynb/Earth_Engine_REST_API_Quickstart.ipynb#scrollTo=6QksNfvaY5em
    # login to Google Cloud
    os.system(F'gcloud auth login --project {project}')
    # define service account credentials
    os.system(F'gcloud iam service-accounts keys create {key} --iam-account {service_account_str}')
    # create authorized session to make HTTP requests
    credentials = service_account.Credentials.from_service_account_file(key)
    scoped_credentials = credentials.with_scopes(
        ['https://www.googleapis.com/auth/cloud-platform'])
    session = AuthorizedSession(scoped_credentials)

    # Authenticate and initialize Earth Engine.
    os.system(F'earthengine authenticate')
    ee.Initialize()
    return session

def get_platform_countries():
    areas_by_platform = pd.read_csv('./platforms/areas_served_by_platform.csv')
    country_codes = pd.read_csv('./platforms/country_codes.csv').rename(columns={'country': 'country_clean'})
    countries = areas_by_platform[['platform', 'country', 'country_clean']].drop_duplicates()
    countries = countries.merge(country_codes, how='left', on=['country_clean'])
    countries = countries[['country', 'alpha3code']].drop_duplicates()
    countries_dict = dict(zip(countries['alpha3code'], countries['country']))
    return countries_dict

def get_country_geojson(country_alpha3):
    # create geojson file from url
    url = F'https://raw.githubusercontent.com/johan/world.geo.json/master/countries/{country_alpha3}.geo.json'
    with ur.request.urlopen(url) as response:
        data = json.loads(response.read().decode())
        return data

def country_poly(country_alpha3):
    geojson = get_country_geojson(country_alpha3)
    coords = geojson['features'][0]['geometry']['coordinates']
    poly = ee.Geometry.Polygon(coords)
    return poly, coords

def get_date_ranges(asset_id):
    if asset_id == 'MODIS/006/MOD11A1':
        # daily images
        # goes back to 2000-03-05 but will go back 10 years
        # API call maxes at 1000 image file names --> 1 year at a time
        start_date = '1/1/2010'
        end_date = '1/1/2020'
        dates = pd.date_range(start=start_date, end=end_date, freq='AS')

    def date_ranges(dates):
        ranges = [[F'{dates[i].year}-01-01T00:00:00.000Z', 
                F'{dates[i+1].year}-01-01T00:00:00.000Z']
                for i in range(len(dates)-1)]
        return ranges

    ranges = date_ranges(dates)
    return ranges

def get_image_ids(session, asset_id, country_alpha3, coords):
    project = 'projects/earthengine-public'
    name = F'{project}/assets/{asset_id}'

    date_ranges = get_date_ranges(asset_id)

    # API maxes at 1000 image file names per call 
    image_ids_sublists = []
    for date_range in tqdm(date_ranges, total=len(date_ranges), desc=F'Getting {country_alpha3} {asset_id} image IDs... '):
        start = date_range[0]
        end = date_range[1]

        url = 'https://earthengine.googleapis.com/v1alpha/{}:listImages?{}'.format(
        name, ur.parse.urlencode({
        'startTime': start,
        'endTime': end,
        'region': '{"type":"Polygon", "coordinates":' + str(coords) + '}',
        }))
        response = session.get(url)
        content_json = json.loads(response.content)
        image_ids_list = list(map(lambda x: x['id'], content_json['images']))
        image_ids_sublists.append(image_ids_list)
        # avoid too many calls too fast
        time.sleep(5)

    image_ids_flat = [item for sublist in image_ids_sublists for item in sublist]
    return image_ids_flat

def get_image_metadata(collection_str, image_id, country_alpha3, poly):
    if collection_str == 'MODIS':
        image = ee.Image(image_id).select('LST_Day_1km').clip(poly) 
        prop = 'system:time_start'
    # to use later for other collections, I know all I want is 1 band and the start time of the image for MODIS
    # properties = image.propertyNames().getInfo()
    # print(properties)

    prop_value = image.get(prop).getInfo()
    df = pd.DataFrame({
        'alpha3code':[country_alpha3],
        'image_id':[image_id],
        prop:[prop_value],
    })
    df['image_timestamp'] = pd.to_datetime(df[prop], unit='ms', utc=True)
    return df


def export_collection_metadata(session, bucket, collection_str, country_alpha3):
    poly, coords = country_poly(country_alpha3)

    if collection_str == 'MODIS':
        # https://developers.google.com/earth-engine/datasets/catalog/MODIS_006_MOD11A1
        asset_id = 'MODIS/006/MOD11A1'

    image_ids = get_image_ids(session, asset_id, country_alpha3, coords)
    collection_metadata = []
    for image_id in tqdm(image_ids, total=len(image_ids), desc=F'Exporting {country_alpha3} {collection_str} metadata... '):
        image_metadata = get_image_metadata(collection_str, image_id, country_alpha3, poly)
        collection_metadata.append(image_metadata)

    collection_metadata = pd.concat(collection_metadata)
    collection_metadata.to_csv(F'gs://{bucket}/earth_engine/metadata/{collection_str}/{country_alpha3}.csv', index=False)

def export_collection_images(bucket, collection_str, country_alpha3):
    # https://colab.research.google.com/github/csaybar/EEwPython/blob/dev/10_Export.ipynb

    poly, coords = country_poly(country_alpha3)
    metadata = pd.read_csv(F'gs://{bucket}/earth_engine/metadata/{collection_str}/{country_alpha3}.csv')

    count = 0
    for image_id in tqdm(metadata['image_id'].unique(), total=metadata['image_id'].nunique(), desc=F'Exporting {country_alpha3} {collection_str} images... '):
        if collection_str == 'MODIS':
            image = ee.Image(image_id).select('LST_Day_1km').clip(poly) 
            image_fn = image_id.replace('/', '-')
            task = ee.batch.Export.image.toCloudStorage(**{
                'image': image,
                'bucket': bucket,
                'fileNamePrefix': F'earth_engine/images_tif/{collection_str}/{country_alpha3}/{image_fn}',
                'region': poly,
                'fileFormat': 'GeoTIFF',
                'scale': 1000,
                'crs': 'EPSG:4326',
                'maxPixels': 1e10
            })
            task.start()
        # avoid too many calls too fast
        time.sleep(5)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--download-metadata", "-dm", dest="download_metadata", action="store_true")
    parser.add_argument("--download-images", "-di", dest="download_images", action="store_true")
    args = parser.parse_args()
    
    session = authenticate(PROJECT, SERVICE_ACCOUNT_STR, KEY)

    countries_dict = get_platform_countries()
    for country_alpha3 in countries_dict.keys():
        if args.download_metadata:
            export_collection_metadata(session, BUCKET, 'MODIS', country_alpha3)
        if args.download_images:
            export_collection_images(BUCKET, 'MODIS', country_alpha3)
