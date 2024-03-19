import os
import datetime as dt
import time

import numpy as np
import xarray as xr


# def create_uber_file(ctd_data_path):
#     rootf = ctd_data_path
#     os.chdir(rootf)
#     dataset_list = []
#     for database_folder in [f.name for f in os.scandir() if f.is_dir() and not f.name.startswith('.')]:
#         os.chdir(database_folder+'/python_processing')
#         if 'ncfiles_id' in os.listdir():
#             os.chdir('ncfiles_id')
#             for file_name in [f.name for f in os.scandir() if f.name.endswith('.nc')]:
#                 ds = xr.open_dataset(file_name)
#                 dataset_list.append(ds)
#         else:
#             print("ncfiles_id does not exist in directory '{}'".format(database_folder))
#         os.chdir(rootf)
#
#     build_nc_file(dataset_list)


def create_uber_file(uber_file, dataset_list):
    string_attrs = ['profile_id', 'chief_scientist', 'platform', 'instrument_type', 'orig_filename', 'orig_header',
                    'station_no', 'datestr', 'timezone', 'timestamp', 'lat', 'lon', 'num_records', 'shallowest_depth',
                    'deepest_depth', 'bottom_depth', ]
    data_lists = {attr: [] for attr in string_attrs}
    for ds in dataset_list:
        for attr in string_attrs:
            data_lists[attr].extend(np.array(ds[attr]))

    ds = xr.Dataset(
        coords=dict(
            timestamp=(['profile'], data_lists['timestamp']),
            lat=(['profile'], data_lists['lat']),
            lon=(['profile'], data_lists['lon']),
        ),
        data_vars=dict(
            profile_id=xr.DataArray(data_lists['profile_id'], dims=['profile']),
            # orig_profile_ID=xr.DataArray(data_lists['orig_profile_id'], dims=['profile']),
            # orig_cruise_ID=xr.DataArray(np.concatenate(), dims=['profile']),
            # access_no=xr.DataArray([], dims=['profile']),
            # cruise_name=xr.DataArray([], dims=['profile']),
            chief_scientist=xr.DataArray(data_lists['chief_scientist'], dims=['profile']),
            platform=xr.DataArray(data_lists['platform'], dims=['profile']),
            instrument_type=xr.DataArray(data_lists['instrument_type'], dims=['profile']),
            orig_filename=xr.DataArray(data_lists['orig_filename'], dims=['profile']),
            orig_header=xr.DataArray(data_lists['orig_header'], dims=['profile']),
            # orig_data_credit=xr.DataArray([], dims=['profile']),
            # doi=xr.DataArray([], dims=['profile']),
            station_no=xr.DataArray(data_lists['station_no'], dims=['profile']),
            datestr=xr.DataArray(data_lists['datestr'], dims=['profile']),
            timezone=xr.DataArray(data_lists['timezone'], dims=['profile']),
            num_records=xr.DataArray(data_lists['num_records'], dims=['profile']),
            shallowest_depth=xr.DataArray(data_lists['shallowest_depth'], dims=['profile']),
            deepest_depth=xr.DataArray(data_lists['deepest_depth'], dims=['profile']),
            bottom_depth=xr.DataArray(data_lists['bottom_depth'], dims=['profile'])
        ),
        attrs=dict(
            creation_date=str(dt.datetime.now().strftime("%Y-%m-%d %H:%M")),
        )
    )

    ds.to_netcdf(uber_file)


def add_new_data_to_uber_file(uber_file, ds_list):
    if os.path.isfile(uber_file):
        print("adding new data")
        ds_list.append(xr.open_dataset(uber_file))
        create_uber_file(uber_file, ds_list)
    else:
        print("File {} do not exist. Creating it".format(uber_file))
