import argparse
import datetime
import glob
import math
import numpy as np
import os
import pandas as pd
import time
import xarray as xr
import rioxarray

from functools import reduce
from scipy.interpolate import interp1d

import sys
sys.path.append('../')
from paths import *
from utils import windpower_simulation_era5_PC


from dask.diagnostics import ProgressBar
ProgressBar().register()
import dask
dask.config.set(**{'array.slicing.split_large_chunks': False})

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)



if not os.path.exists(results_path):
    os.makedirs(results_path)

parser = argparse.ArgumentParser()
parser.add_argument('-turbine', help='Insert turbine model')
parser.add_argument('-startyear', help='Insert starting year for simulation')
parser.add_argument('-endyear', help='Insert ending year for simulation')
args = parser.parse_args()
turbine_model = args.turbine
startyear = args.startyear
endyear = args.endyear

# check if data were supplied
if turbine_model == None:
    print('Error: Turbine model missing')
    exit()
if startyear == None:
    print('Error: Startyear missing')
    exit()
if endyear == None:
    print('Error: Endyear missing')
    exit()

# get turbine data and check turbine model
turbine_data = pd.read_csv(windsim_data_path + '/windturbines.csv')
if turbine_model not in turbine_data.type.values:
    print('Invalid turbine type!')
    exit()

# start and enddate for GWA3
startGWA = '2008'
endGWA = '2017'

# get locations
locations = pd.read_csv(windsim_data_path + '/locations.csv')

# Simulate wind power with ERA5
wind = xr.open_mfdataset(era_path_eff + "/era5_wind_AUT_*.nc", chunks = {'time': 140})
alpha = xr.open_mfdataset(era_path_eff + "/era5_alpha_AUT_*.nc", chunks = {'time': 140})

# check if period long enough for GWA and desired simulation period
if (pd.DatetimeIndex(wind.time.values)[0].year > int(startGWA)) or (pd.DatetimeIndex(wind.time.values)[-1].year < int(endGWA)):
    var = input('Warning: period not sufficient for GWA. Continue? y/n  ')
    if var == 'n':
        exit()
    if var != 'y':
        print('Error: invalid answer')
        exit()
if (pd.DatetimeIndex(wind.time.values)[0].year > int(startyear)) or (pd.DatetimeIndex(wind.time.values)[-1].year < int(endyear)):
    var = input('Warning: period not sufficient for desired simulation period. Continue? y/n  ')
    if var == 'n':
        exit()
    if var != 'y':
        print('Error: invalid answer')
        exit()

# get power curve
turbine = turbine_data.loc[np.where(turbine_data.type==turbine_model)[0][0]]
ttype = turbine.type
height = turbine.height
power = turbine['v0':'v25']
windspeed = range(len(power))
cutout = max(windspeed)

if not os.path.exists(results_path):
    os.mkdir(results_path)

outfile = results_path + '/windpower_AUT_ERA5*.nc'

if results_path + '/windpower_AUT_ERA5_GWA3_' + ttype + '_' + startyear + '-' + endyear + '.nc' not in glob.glob(outfile):
	print('calculating windpower ERA5 AUT GWA3 with turbine type' + ttype)
	#GWA = xr.open_rasterio(windsim_data_path+'/AUT_wind-speed_100m.tif')
	GWA = rioxarray.open_rasterio(windsim_data_path+'/AUT_wind-speed_100m.tif')
	wps = windpower_simulation_era5_PC(wind.wh100,
                                       alpha.alpha,
									   height,
                                       windspeed,
                                       power.values,
                                       cutout,
									   turbine.cap,
									   locations.lon.values,
									   locations.lat.values,
                                       locations.ind.values,
									   startyear,
                                       endyear,
									   GWA,
                                       startGWA,
                                       endGWA)
	# save as netcdf
	wps.to_dataset(name='wp').to_netcdf(results_path+"/windpower_AUT_ERA5_GWA3_"+ttype+"_"+startyear+"-"+endyear+".nc")
