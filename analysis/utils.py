import json
import os
import re
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from bokeh.io import output_notebook, show, output_file, export_png
from bokeh.plotting import figure
from bokeh.models import GeoJSONDataSource, LinearColorMapper, ColorBar
from bokeh.palettes import all_palettes


def data_to_local(collection, bucket='1182020'):
    # copy metadata from GCP to local
    os.system(F'gsutil -m -q cp -r gs://{bucket}/earth_engine/metadata/{collection} ./metadata')
    # copy images from GCP to local
    os.system(F'gsutil -m -q cp -r gs://{bucket}/earth_engine/images_tif/{collection} ./images_tif')
    

def country_platform_info(repo_path):
    country_codes = pd.read_csv(F'{repo_path}/data/country_info/country_codes.csv', keep_default_na=False).rename(columns={'country': 'country_clean'})
    country_continents = pd.read_csv(F'{repo_path}/data/country_info/country_continents.csv', keep_default_na=False)[['continet_code', 'alpha3code']]
    country_areas = pd.read_csv(F'{repo_path}/data/country_info/country_areas.csv', keep_default_na=False)[['country_clean', 'area_km2']]

    country_info = country_codes.merge(country_continents, how='left', on=['alpha3code'])
    country_info = country_info.merge(country_areas, how='left', on=['country_clean'])

    areas_by_platform = pd.read_csv(F'{repo_path}/data/platforms/areas_served_by_platform.csv')
    countries_by_platform = areas_by_platform[['platform', 'country', 'country_clean']].drop_duplicates()
    country_platform_info = countries_by_platform.merge(country_info, how='left', on=['country_clean'])

    return country_platform_info


def countries_with_data(collection):
    # get countries that have metadata in path (images should be there if metadata is)
    p = re.compile('[A-Z]{3}')
    countries = [country.replace('.csv', '') for country in os.listdir(F'./metadata/{collection}') if p.match(country)]
    return countries


def country_shapes_by_platform(repo_path, platform, countries_platforms):
    continents = pd.read_csv(F'{repo_path}/data/country_info/country_continents.csv')
    # https://www.naturalearthdata.com/downloads/110m-cultural-vectors/
    shapefile = F'{repo_path}/data/country_shapefiles/ne_110m_admin_0_countries.shp'
    gdf = gpd.read_file(shapefile)[['ADMIN', 'ADM0_A3', 'geometry']]
    gdf.columns = ['country', 'alpha3code', 'geometry']

    if platform == 'afr100':
        # get all countries in Africa
        afr_countries = continents[continents['continet_code'] == 'AF']['alpha3code'].unique()
        gdf = gdf[gdf['alpha3code'].isin(afr_countries)]
    elif platform == 'cities4forests':
        # get all countries in Europe
        na_eur_countries = list(continents[continents['continet_code'] == 'EU']['alpha3code'].unique())
        # remove countries that have no data and skew map
        na_eur_countries.remove('RUS')
        na_eur_countries.remove('KAZ')
        na_eur_countries.remove('ISL')
        na_eur_countries.remove('SJM')
        na_eur_countries.remove('FRO')
        na_eur_countries.remove('NOR')
        na_eur_countries.remove('FRA')
        gdf = gdf[gdf['alpha3code'].isin(na_eur_countries)]
    elif platform == 'initative20x20':
        # get all countries in South America
        sa_countries = continents[continents['continet_code'] == 'SA']['alpha3code'].unique()
        # get all countries in initative 20x20
        platform_countries = countries_platforms[countries_platforms['platform'] == platform]['alpha3code'].unique()
        platform_countries = list(platform_countries) + list(sa_countries)
        gdf = gdf[gdf['alpha3code'].isin(platform_countries)]

    return gdf


def visualize_country_platform_changes(collection, platform, viz_df, column_name, min_change, max_change, title, palette, reverse_palette=False):
    # visualization code based on source below
    # https://towardsdatascience.com/a-complete-guide-to-an-interactive-geographical-map-using-python-f4c5197e23e0
    
    viz_json = json.loads(viz_df.to_json())
    json_data = json.dumps(viz_json)

    geosource = GeoJSONDataSource(geojson = json_data)

    palette = all_palettes[palette][6]
    if reverse_palette:
        palette = palette[::-1]

    color_mapper = LinearColorMapper(palette = palette, low = min_change, high = max_change)
    color_bar = ColorBar(color_mapper=color_mapper, label_standoff=8,width = 400, height = 20, \
                            border_line_color=None,location = (0,0), orientation = 'horizontal') 
    p = figure(title = F'{platform} {title}', plot_height = 600 , plot_width = 500, toolbar_location = None)
    p.xgrid.grid_line_color = None
    p.ygrid.grid_line_color = None
    p.patches('xs','ys', source = geosource,fill_color = {'field' : column_name, 'transform' : color_mapper},
              line_color = 'black', line_width = 0.25, fill_alpha = 1)
    p.add_layout(color_bar, 'below')
    output_notebook()
    show(p)
    export_png(p, filename=F"./output/{platform}_{column_name}.png")


def compare_platforms_area_curve(df_by_country, df_by_platform, column_name, title):
    # visualization code based on source below
    # https://python-graph-gallery.com/242-area-chart-and-faceting/

    # for stats
    platforms = ['afr100', 'cities4forests', 'initative20x20']
    for platform in platforms:
        try:
            df_ = df_by_country[df_by_country['platform'] == platform]
        except KeyError:
            df_ = df_by_country[(df_by_country['platform_1'] == platform) | (df_by_country['platform_2'] == platform)]
        print(F'{platform}: {df_["country"].nunique()} countries in sample')
        
    df = df_by_platform.rename(columns={column_name: title})
    start_year = df['year'].min()
    end_year = df['year'].max()

    sns.set(style="darkgrid")
    g = sns.FacetGrid(df, col='platform', hue='platform', col_wrap=4, )
    g = g.map(plt.plot, 'year', title)
    g = g.map(plt.fill_between, 'year', title, alpha=0.2).set_titles("{col_name} platform")
    g = g.set_titles("{col_name}")
    g = g.set(xticks=list(range(start_year, end_year+1)))

    plt.subplots_adjust(left=0.2, top=0.8)
    unit_ind = title.index(' [')
    supertitle = F'Change in {title[:unit_ind]}'
    g = g.fig.suptitle(supertitle)

    plt.savefig(F"./output/{column_name}.png")
    plt.show()
    


def change_by_country_platform(column_name, start_year, end_year, df_by_country, platform, countries_platforms):
    df = df_by_country.groupby(['alpha3code', 'year'])[column_name].mean().reset_index()
    country_dfs = []
    for country in df['alpha3code'].unique():
        country_df = df[df['alpha3code'] == country]
        data_start = country_df[country_df['year'] == start_year][column_name].iloc[0]
        data_end = country_df[country_df['year'] == end_year][column_name].iloc[0]
        data_change = data_end - data_start
        df_ = pd.DataFrame({'alpha3code': [country], F'{column_name}_{start_year}_{end_year}': [data_change]})
        country_dfs.append(df_)
    country_df = pd.concat(country_dfs)
    # merge back with country info, filter for platforms
    df = country_df.merge(countries_platforms, how='left', on=['alpha3code'])
    df = df[df['platform'] == platform]
    return df

def get_visualization_df(column_name, start_year, end_year, repo_path, df_by_country, platform, countries_platforms):
    df = change_by_country_platform(column_name, start_year, end_year, df_by_country, platform, countries_platforms)
    col_name = F'{column_name}_{start_year}_{end_year}'
    min_change = df[col_name].min()
    max_change = df[col_name].max()
    gdf = country_shapes_by_platform(repo_path, platform, countries_platforms)
    df = gdf.merge(df, how='left', on=['alpha3code'])
    return df, min_change, max_change