# -*- coding: utf-8 -*-
"""
Created on Mon Apr 29 09:00:37 2019

@author: KatharinaG

adapted from source:
https://github.com/inwe-boku/wind_repower_usa/blob/master/scripts/download_wind_era5.py
"""
import glob
import os
import os.path as op

import cdsapi
import sys
sys.path.append('../')

from multiprocessing import Pool

from paths import era_path

if not os.path.isdir(era_path):
    os.makedirs(era_path)
    print('Created directory ',era_path)

DOWNLOAD_DIR = era_path
COUNTRY = 'AUT'

YEARS = range(2021,1949,-1)
MONTHS = list(range(1, 13))

north = 50
south = 45.5
east = 18
west = 8



def download_era5(month):
    # API documentation for downloading a subset:
    # https://confluence.ecmwf.int/display/CKB/Global+data%3A+Download+data+from+ECMWF+for+a+particular+area+and+resolution
    # https://retostauffer.org/code/Download-ERA5/

    # Format for downloading ERA5: North/West/South/East
    bounding_box = "{}/{}/{}/{}".format(north, west, south, east)

    print("Downloading bounding_box=%s for years=%s and months=%s",
                 bounding_box, YEAR, month)

    c = cdsapi.Client()
    
    filename = op.join(DOWNLOAD_DIR,
                       'era5_wind_' + COUNTRY + f'_{YEAR}{month:02d}.nc')

    if op.exists(filename):
        print(f"Skipping {filename}, already exists!")
        quit()

    print(f"Starting download of {filename}...")
    
    if YEAR < 1979:
        dataset = 'reanalysis-era5-single-levels-preliminary-back-extension'
    else:
        dataset = 'reanalysis-era5-single-levels'
    
    for i in range(5):
        try:
            c.retrieve(
                dataset,
                {
                    'product_type': 'reanalysis',
                    'format': 'netcdf',
                    'variable': [
                        '100m_u_component_of_wind',
                        '100m_v_component_of_wind',
                        '10m_u_component_of_wind',
                        '10m_v_component_of_wind'
                    ],
                    'year': f'{YEAR}',
                    'month': [
                        f'{month:02d}'
                    ],
                    'area': bounding_box,
                    'day': [f"{day:02d}" for day in range(1, 32)],
                    'time': [f"{hour:02d}:00" for hour in range(24)],
                },
                f"{filename}.part"
            )
        except Exception as e:
            print("Download failed: %s", e)
        else:
            print(f"Download of {filename} successful!")
            os.rename(f"{filename}.part", filename)
            break
    else:
        print("Download failed permanently!")



if __name__ == '__main__':
    for YEAR in YEARS:
        if len(glob.glob(DOWNLOAD_DIR+'/era5_wind_' + COUNTRY + '_' + str(YEAR) + '??.nc')) == 12:
            print('Skipping',YEAR,' already there...')
        else:
            pool = Pool()
            pool.map(download_era5,MONTHS)
