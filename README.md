# omdena-wri

This repository contains Python scripts, notebooks, and csvs for geospatial analysis and visualizations of platform hazards. All documentation is saved within the main Omdena-WRI Google Drive folder.

## earth_engine
* Code to export country-specifc metadata and images from Earth Engine to a Google Cloud Platform bucket is saved here.

To run export of metadata in command line:

`python -m export_images_by_country -c $COLLECTION_STR -dm`
To run export of images in command line:

`python -m export_images_by_country -c $COLLECTION_STR -di`

## Supported Earth Engine collections (options for COLLECTION_STR):

* MODIS_LST_day
* MODIS_LST_8day
* MODIS_land_cover
* hansen_forest_change
* SMAP_soil_moisture

## analysis

Analysis notebooks, including code to process the images from GCP, lives under the `analysis/` subfolder. All processed Earth Engine collection csvs and visualizations are saved under `analysis/output/`. Most common functions across the notebooks have been moved to `analysis/utils.py`. Some code will not run without gaining access to the current GCP project.

## GCP
The GCP project and bucket is currently registered to a free trial account. Earth Engine code will not run without modifying to new GCP credenitals or gaining access to the current project.

Created by Grace Mitchell, gmm93@cornell.edu
