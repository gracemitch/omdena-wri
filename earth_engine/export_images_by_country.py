import argparse
import ee
import json
import os
import numpy as np
import pandas as pd
import time
from tqdm import tqdm
import urllib as ur
from google.cloud import storage
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account
from multiprocessing import Process

PROJECT = 'omdena-wri'
SERVICE_ACCOUNT_STR ='sa1-311@omdena-wri.iam.gserviceaccount.com'
KEY = 'private-key.json'
BUCKET = '1182020'


def get_session(project, service_account_str, key, collection):
    if collection == 'hansen_forest_change':
        return None
    
    # only needed to get full list of image ids

    # # # GCP instructions
    # # # 1. Install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/quickstarts)

    # # https://colab.research.google.com/github/google/earthengine-api/blob/master/python/examples/ipynb/Earth_Engine_REST_API_Quickstart.ipynb#scrollTo=6QksNfvaY5em
    # login to Google Cloud
    os.system(F'gcloud auth login --project {project}')
    # define service account credentials
    os.system(F'gcloud iam service-accounts keys create {key} --iam-account {service_account_str}')
    # create authorized session to make HTTP requests
    credentials = service_account.Credentials.from_service_account_file(key)
    scoped_credentials = credentials.with_scopes(
        ['https://www.googleapis.com/auth/cloud-platform'])
    session = AuthorizedSession(scoped_credentials)
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
    # source: https://github.com/johan/world.geo.json/tree/master/countries
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
    elif asset_id == 'MODIS/006/MOD11A2':
        # each image is an average of last 8 days
        # goes back to 2000-03-05 but will go back 5 years
        # API call maxes at 1000 image file names --> 1 year at a time
        start_date = '1/1/2015'
        end_date = '1/1/2020'
        dates = pd.date_range(start=start_date, end=end_date, freq='AS')
    elif asset_id == 'MODIS/006/MCD12Q1':
        # annual images
        start_date = '1/1/2001'
        end_date = '1/1/2020'
        dates = pd.date_range(start=start_date, end=end_date, freq='AS')
    elif asset_id == 'NASA_USDA/HSL/SMAP_soil_moisture':
        # image every 3 days
        # goes back to 2015-04-01 but will go back 3 years
        start_date = '1/1/2017'
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
        time.sleep(1)

    image_ids_flat = [item for sublist in image_ids_sublists for item in sublist]
    return image_ids_flat


def get_image_metadata(collection_str, image_id, country_alpha3, poly):
    if collection_str in ['MODIS_LST_day',  'MODIS_LST_8day']:
        image = ee.Image(image_id).select('LST_Day_1km').clip(poly) 
        properties = ['system:time_start', 'system:bands', 'system:band_names']
    elif collection_str == 'MODIS_land_cover':
        image = ee.Image(image_id).clip(poly) 
        properties = ['system:time_start', 'system:version', 'system:bands', 'system:band_names']
    else:
        image = ee.Image(image_id).clip(poly) 
        properties = image.propertyNames().getInfo()

    df = pd.DataFrame({
            'alpha3code':[country_alpha3],
            'image_id':[image_id],
        })

    for prop in properties:
        prop_value = image.get(prop).getInfo()
        if prop == 'system:time_start':
            df[prop] = prop_value
            df['image_timestamp'] = pd.to_datetime(df[prop], unit='ms', utc=True)
        else:
            if type(prop_value) not in [str, int, float]:
                # assume list type or dict
                prop_value = [prop_value]
            df[prop] = prop_value 
    return df


def get_file_names(bucket_str, path, file_extension):
    client = storage.Client()
    bucket = client.bucket(bucket_str)
    blobs = list(bucket.list_blobs(prefix=path))
    return [blob.name for blob in blobs if blob.name.endswith(file_extension)]


def export_collection_metadata(bucket, collection_str, country_alpha3, session=None):
    poly, coords = country_poly(country_alpha3)

    if collection_str == 'MODIS_LST_day':
        # https://developers.google.com/earth-engine/datasets/catalog/MODIS_006_MOD11A1
        asset_id = 'MODIS/006/MOD11A1'
        image_ids = get_image_ids(session, asset_id, country_alpha3, coords)
    elif collection_str == 'MODIS_LST_8day':
        # https://developers.google.com/earth-engine/datasets/catalog/MODIS_006_MOD11A2
        asset_id = 'MODIS/006/MOD11A2'
        image_ids = get_image_ids(session, asset_id, country_alpha3, coords)
    elif collection_str == 'MODIS_land_cover':
        # https://developers.google.com/earth-engine/datasets/catalog/MODIS_006_MCD12Q1
        asset_id = 'MODIS/006/MCD12Q1'
        image_ids = get_image_ids(session, asset_id, country_alpha3, coords)
    elif collection_str == 'hansen_forest_change':
        # https://developers.google.com/earth-engine/datasets/catalog/UMD_hansen_global_forest_change_2019_v1_7
        asset_id = 'UMD/hansen/global_forest_change_2019_v1_7'
        image_ids = ['UMD/hansen/global_forest_change_2019_v1_7']
    elif collection_str == 'SMAP_soil_moisture':
        # https://developers.google.com/earth-engine/datasets/catalog/NASA_USDA_HSL_SMAP_soil_moisture
        asset_id = 'NASA_USDA/HSL/SMAP_soil_moisture'
        image_ids = get_image_ids(session, asset_id, country_alpha3, coords)
    
    collection_metadata = []
    for image_id in tqdm(image_ids, total=len(image_ids), desc=F'Exporting {country_alpha3} {collection_str} metadata... '):
        image_metadata = get_image_metadata(collection_str, image_id, country_alpha3, poly)
        collection_metadata.append(image_metadata)

    collection_metadata = pd.concat(collection_metadata)
    collection_metadata.to_csv(F'gs://{bucket}/earth_engine/metadata/{collection_str}/{country_alpha3}.csv', index=False)


def get_missing_images(image_path, metadata, bucket, country_alpha3):
    images_desired = list(map(lambda x: F"{image_path}{x.replace('/', '-')}.tif", metadata['image_id']))
    images_complete = get_file_names(bucket, image_path, '.tif')
    images_missing = np.setdiff1d(images_desired, images_complete)
    print(F'{len(images_missing)}/{len(images_desired)} images remaining {country_alpha3}...')
    return list(map(lambda x: F"{x.replace('-', '/').replace('.tif', '')}", images_missing))


def export_collection_images(bucket, collection_str, country_alpha3, years=None):
    # https://colab.research.google.com/github/csaybar/EEwPython/blob/dev/10_Export.ipynb

    ee.Initialize()
    
    poly, coords = country_poly(country_alpha3)
    metadata = pd.read_csv(F'gs://{bucket}/earth_engine/metadata/{collection_str}/{country_alpha3}.csv')

    if collection_str == 'MODIS_LST_day':
        assert years <= 9, F'Metdata collected starts 1/1/2010 and ends 12/31/2019.'
        # filter metadata for past n years only
        metadata['image_timestamp'] = pd.to_datetime(metadata['image_timestamp'], infer_datetime_format=True)
        filter_date = pd.to_datetime(F'{2010+years}-01-01 00:00:00+00:00', infer_datetime_format=True)
        metadata = metadata[metadata['image_timestamp'] >= filter_date]

    # missing images
    image_path = F'earth_engine/images_tif/{collection_str}/{country_alpha3}/'
    images_missing = get_missing_images(image_path, metadata, bucket, country_alpha3)

    if len(images_missing) > 0:
        for image_id in tqdm(images_missing, total=len(images_missing), desc=F'Exporting {country_alpha3} {collection_str} images... '):
            ee_image_id = image_id.replace(image_path, '')
            image_fn = ee_image_id.replace('/', '-')
            image_fp = F'earth_engine/images_tif/{collection_str}/{country_alpha3}/{image_fn}'

            if collection_str in ['MODIS_LST_day',  'MODIS_LST_8day']:
                image = ee.Image(ee_image_id).select('LST_Day_1km').clip(poly) 
                task = ee.batch.Export.image.toCloudStorage(**{
                'image': image,
                'bucket': bucket,
                'fileNamePrefix': image_fp,
                'region': poly,
                'fileFormat': 'GeoTIFF',
                'scale': 1000,
                'crs': 'EPSG:4326',
                'maxPixels': 1e10
                })
            else:
                image = ee.Image(ee_image_id).clip(poly) 
                task = ee.batch.Export.image.toCloudStorage(**{
                'image': image,
                'bucket': bucket,
                'fileNamePrefix': image_fp,
                'region': poly,
                'fileFormat': 'GeoTIFF',
                'maxPixels': 5e10
                })

            task.start()

            while task.status()['state'] in ['READY', 'RUNNING']:
                time.sleep(5)
            else:
                print(task.status())
                continue


def get_missing_metadata(countries_dict, bucket, collection_str):
    metadata_path = F'earth_engine/metadata/{collection_str}/'
    metadata_desired = list(map(lambda x: F'{metadata_path}{x}.csv', countries_dict.keys()))
    metadata_complete = get_file_names(bucket, metadata_path, '.csv')
    metadata_missing = np.setdiff1d(metadata_desired, metadata_complete)
    print(F'{len(metadata_missing)}/{len(metadata_desired)} metadata files remaining...')
    # country alpha 3 code
    return list(map(lambda x: F"{x.replace(metadata_path, '').replace('.csv', '')}", metadata_missing))

def get_countries_with_complete_metadata(countries_dict, BUCKET, COLLECTION):
    missing_countries = get_missing_metadata(countries_dict, BUCKET, COLLECTION)
    complete_countries = np.setdiff1d(list(countries_dict.keys()), missing_countries)
    return complete_countries


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # TODO: rename MODIS to MODIS_land_temperature
    parser.add_argument("--collection", "-c", type=str, required=True, \
    help='MODIS_LST_day, MODIS_LST_8day, MODIS_land_cover, hansen_forest_change, or SMAP_soil_moisture')
    parser.add_argument("--download-metadata", "-dm", dest="download_metadata", action="store_true")
    parser.add_argument("--download-images", "-di", dest="download_images", action="store_true")
    args = parser.parse_args()
    collection = args.collection
    assert args.collection in [
        'MODIS_LST_day', 
        'MODIS_LST_8day', 
        'MODIS_land_cover',
        'hansen_forest_change',
        'SMAP_soil_moisture'
    ], F'Earth Engine collection {collection} not supported.'

    # # # TESTING
    # ee.Initialize()
    # session = get_session(PROJECT, SERVICE_ACCOUNT_STR, KEY, collection)
    # countries_dict = get_platform_countries()
    # countries = get_missing_metadata(countries_dict, BUCKET, collection)
    # country = countries[0]
    # export_collection_metadata(BUCKET, collection, country, session)

    countries_dict = get_platform_countries()

    if args.download_metadata:
        ee.Initialize()
        session = get_session(PROJECT, SERVICE_ACCOUNT_STR, KEY, collection)
        countries = get_missing_metadata(countries_dict, BUCKET, collection)
        metadata_logs = []
        for country_alpha3 in countries:
            error = []
            try:
                export_collection_metadata(BUCKET, collection, country_alpha3, session)
                status = 'SUCCESS'
                error.append(None)
            except Exception as e:
                print(country_alpha3, e)
                status = 'FAIL'
                error.append(e)
                continue
            finally:
                print(country_alpha3, error)
                metadata_log = pd.DataFrame({
                    'country_alpha3': [country_alpha3], 
                    'status': [status], 
                    'exception': error,
                })
                metadata_logs.append(metadata_log)
        metadata_log = pd.concat(metadata_logs)
        metadata_log.to_csv(F'{collection}_metadata_log.csv', index=False)
    
    if args.download_images:
        processes = []
        countries = get_countries_with_complete_metadata(countries_dict, BUCKET, collection)
        for country_alpha3 in countries:
            if collection == 'MODIS_LST_day':
                p = Process(target=export_collection_images, args=(BUCKET, collection, country_alpha3, 5))
            else:
                p = Process(target=export_collection_images, args=(BUCKET, collection, country_alpha3))

            p.start()
            processes.append(p)

        for p in processes:
            p.join()