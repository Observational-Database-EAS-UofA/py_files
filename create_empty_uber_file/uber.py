import os
import datetime as dt
import time

import numpy as np
import xarray as xr


def create_uber_file(ctd_data_path):
    rootf = ctd_data_path
    os.chdir(rootf)
    dataset_list = []
    for database_folder in [f.name for f in os.scandir() if f.is_dir() and not f.name.startswith('.')]:
        os.chdir(database_folder)
        if 'ncfiles_id' in os.listdir():
            os.chdir('ncfiles_id')
            for file_name in [f.name for f in os.scandir() if f.name.endswith('.nc')]:
                ds = xr.open_dataset(file_name)
                dataset_list.append(ds)
        os.chdir(rootf)

    build_nc_file(dataset_list)


def build_nc_file(dataset_list):
    profile_id_list = []
    orig_filename_list = []
    orig_header_list = []
    platform_list = []
    chief_scientist_list = []
    instrument_type_list = []
    station_no_list = []
    lon_list = []
    lat_list = []
    num_records_list = []
    bottom_depth_list = []
    shallowest_depth_list = []
    deepest_depth_list = []
    datestr_list = []
    timestamp_list = []

    for ds in dataset_list:
        profile_id_list.append(ds['profile_id'].values)
        orig_filename_list.append(ds['orig_filename'].values)
        orig_header_list.append(ds['orig_header'].values)
        platform_list.append(ds['platform'].values)
        chief_scientist_list.append(ds['chief_scientist'].values)
        instrument_type_list.append(ds['instrument_type'].values)
        station_no_list.append(ds['station_no'].values)
        lon_list.append(ds['lon'].values)
        lat_list.append(ds['lat'].values)
        num_records_list.append(ds['num_records'].values)
        bottom_depth_list.append(ds['bottom_depth'].values)
        shallowest_depth_list.append(ds['shallowest_depth'].values)
        deepest_depth_list.append(ds['deepest_depth'].values)
        datestr_list.append(ds['datestr'].values)
        timestamp_list.append(ds['timestamp'].values)

    ds = xr.Dataset(
        coords=dict(
            timestamp=(['profile'], np.concatenate(timestamp_list)),
            lat=(['profile'], np.concatenate(lat_list)),
            lon=(['profile'], np.concatenate(lon_list)),
        ),
        data_vars=dict(
            profile_id=xr.DataArray(np.concatenate(profile_id_list), dims=['profile']),
            orig_profile_ID=xr.DataArray(np.concatenate(orig_filename_list), dims=['profile']),
            # orig_cruise_ID=xr.DataArray(np.concatenate(), dims=['profile']),
            # access_no=xr.DataArray([], dims=['profile']),
            # cruise_name=xr.DataArray([], dims=['profile']),
            chief_scientist=xr.DataArray(np.concatenate(chief_scientist_list), dims=['profile']),
            platform=xr.DataArray(np.concatenate(platform_list), dims=['profile']),
            instrument_type=xr.DataArray(np.concatenate(instrument_type_list), dims=['profile']),
            orig_filename=xr.DataArray(np.concatenate(orig_filename_list), dims=['profile']),
            orig_header=xr.DataArray(np.concatenate(orig_header_list), dims=['profile']),
            # orig_data_credit=xr.DataArray([], dims=['profile']),
            # doi=xr.DataArray([], dims=['profile']),
            station_no=xr.DataArray(np.concatenate(station_no_list), dims=['profile']),
            datestr=xr.DataArray(np.concatenate(datestr_list), dims=['profile']),
            num_records=xr.DataArray(np.concatenate(num_records_list), dims=['profile']),
            shallowest_depth=xr.DataArray(np.concatenate(shallowest_depth_list), dims=['profile']),
            deepest_depth=xr.DataArray(np.concatenate(deepest_depth_list), dims=['profile']),
            bottom_depth=xr.DataArray(np.concatenate(bottom_depth_list), dims=['profile'])
        ),
        attrs=dict(
            creation_date=str(dt.datetime.now().strftime("%Y-%m-%d %H:%M")),
        )
    )

    os.chdir("..")
    ds.to_netcdf("uber_file.nc")


def add_new_data_to_uber_file(uber_file, ds_list):
    try:
        uber_ds = xr.open_dataset(uber_file)
    except Exception as e:
        print(e)
        print("File {} do not exist. Creating it".format(uber_file))
