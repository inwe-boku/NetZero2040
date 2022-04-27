import xarray as xr
import numpy as np
from scipy.interpolate import interp1d
import pandas as pd


def windpower_simulation_era5_PC(windh100,alpha,hubheight,windspeed,power,cutout,capacity,lons,lats,ids,start,end,GWA=[],startGWA=0,endGWA=0):
    '''
    function for simulating wind power generation
    
    input parameters:
    
    windh100	wind speeds dataset with effective wind speeds in one height (100m)
    alpha		alpha friction coefficient calculate from wind speeds in two heights
    hubheight	vector of hub height of different turbines
    capacity	installed capacity of turbines
    lons, lats	locations of wind power plants
    ids         epe ids of the locations
    start, end	start and end of wind power simulation
    GWA			empty list by default, if wind speed correction with GWA desired provide GWA
    '''
    
    # interpolate wind to locations of turbines
    wind = windh100.sel(time=slice(start,end)).interp(coords={"longitude":xr.DataArray(lons,dims='id',coords={'id':ids}),
                                                              "latitude":xr.DataArray(lats,dims='id',coords={'id':ids})},
                                                      method="nearest").compute()
    # if GWA is used compute mean wind speed for GWA
    if(len(GWA)>0):
        mwind = windh100.sel(time=slice(startGWA,endGWA)).interp(coords={"longitude":xr.DataArray(lons,dims='id',coords={'id':ids}),
                                                                         "latitude":xr.DataArray(lats,dims='id',coords={'id':ids})},
                                                                 method="nearest").mean('time').compute()
    # interpolate alpha to locations of turbines
    alphai = alpha.sel(time=slice(start,end)).interp(coords={"longitude":xr.DataArray(lons,dims='id',coords={'id':ids}),
                                                             "latitude":xr.DataArray(lats,dims='id',coords={'id':ids})},
                                                     method="nearest").compute()
    # calculate wind at hubheight using alpha
    windhh = (wind * (hubheight/100)**alphai).compute()
    
    # apply GWA bias correction
    if(len(GWA)>0):
        # interpolate to turbine locations
        GWA_locations = GWA.interp(coords={"x":xr.DataArray(lons,dims='id',coords={'id':ids}),
                                           "y":xr.DataArray(lats,dims='id',coords={'id':ids})},
                                   method="nearest").compute()
        # calculate correction factor
        cf_GWA = (GWA_locations/mwind).compute()
        # apply correction factor
        windhhg = (windhh * cf_GWA).compute()
        # replace wind speeds higher than 25 m/s with 0, because cutout windspeed
        windhhg = windhhg.where(windhhg<=cutout,0)
    else:
        windhhg = windhh.where(windhh<=cutout,0)
    
    # apply power curve
    PC = interp1d(windspeed, power)
    wp1 = xr.apply_ufunc(PC, windhhg,
                         dask='parallelized',
                         output_dtypes=[np.float64])
    
    
    # multiply with installed capacity
    wp2 = capacity*wp1/max(power)
    return(wp2)
