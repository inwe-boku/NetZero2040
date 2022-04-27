# Script for preparing ERA5 reanalysis data:
# 1. Calculate effective wind speeds from u and v wind speeds in two heights (10 and 100m)
# 2. Calculate alpha friction coefficient

import glob
import numpy as np
import os
import xarray as xr

import sys
sys.path.append('../')
from paths import *

from dask.diagnostics import ProgressBar
ProgressBar().register()

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

if not os.path.exists(era_path_eff):
    os.mkdir(era_path_eff)

out_files = glob.glob(era_path_eff + '/*')

for YEAR in range(1950,2022):
    year = str(YEAR)
    wfile = era_path_eff+'/era5_wind_AUT_' + year + '.nc'
    afile = era_path_eff+'/era5_alpha_AUT_' + year + '.nc'
    files = era_path+'/era5_wind_AUT_'+year+'??.nc'
    if len(glob.glob(files))<12:
        print('Skipping',year,' Not complete...')
    else:
        if wfile not in out_files:
            print('calculating wind ' + year)
            data = xr.open_mfdataset(files, chunks = {'time': 140})
            wh10 = ((data.u10**2+data.v10**2)**0.5).compute()
            wh100 = ((data.u100**2+data.v100**2)**0.5).compute()
            print('saving wind ' + year)
            eff_ws = xr.Dataset({'wh10': wh10,
                                 'wh100': wh100})
            eff_ws.to_netcdf(wfile)
        else:
            print('Skipping',year,' Already there')
        if afile not in out_files:
            print('calculating alpha ' + year)
            eff_ws = xr.open_dataset(wfile)
            alpha = (xr.ufuncs.log(eff_ws.wh100/eff_ws.wh10)/np.log(100/10)).compute()
            print('saving alpha ' + year)
            xr.Dataset({'alpha': alpha}).to_netcdf(afile)