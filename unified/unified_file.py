import os
import datetime as dt
import time

import numpy as np
import xarray as xr


def create_unified_file(unified_file, dataset_list):
    string_attrs = ['profile_id', 'cruise_name', 'chief_scientist', 'platform', 'instrument_type', 'orig_filename',
                    'orig_header', 'station_no', 'datestr', 'timezone', 'timestamp', 'lat', 'lon', 'num_records',
                    'shallowest_depth', 'deepest_depth', 'bottom_depth', ]
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
            **{attr: xr.DataArray(data_lists[attr], dims=['profile']) for attr in string_attrs if
               attr not in ['timestamp', 'lon', 'lat']},
        ),
        attrs=dict(
            creation_date=str(dt.datetime.now().strftime("%Y-%m-%d %H:%M")),
        )
    )

    ds.to_netcdf(unified_file)


def add_new_data_to_unified_file(unified_file, ds_list):
    if os.path.isfile(unified_file):
        print("adding new data")
        ds_list.append(xr.open_dataset(unified_file))
        create_unified_file(unified_file, ds_list)
    else:
        print("File {} do not exist. Creating it".format(unified_file))
