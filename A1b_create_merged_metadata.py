"""
This code manages the creation and updating of a merged dataset file from multiple NetCDF datasets.
The key operations include:
1. Creating a merged file from a list of datasets.
2. Adding new datasets to an existing merged file.

Functions in the script:
1. create_merged_file: Create a merged file by combining information from a list of datasets.
2. add_new_data_to_merged_file: Add new data to an existing merged file by appending dataset information.

"""

import os
import datetime as dt
import time

import numpy as np
import xarray as xr


def create_merged_file(merged_file, dataset_list):
    """
    Create a merged file by combining information from a list of datasets.

    Args:
        merged_file (str): Path to the merged file.
        dataset_list (list): List of datasets containing information to be merged.
    """
    string_attrs = [
        "profile_id",
        "creation_date",
        "orig_profile_id",
        "orig_cruise_id",
        "access_no",
        "dataset_name",
        "cruise_name",
        "chief_scientist",
        "platform",
        "instrument_type",
        "orig_filename",
        "orig_data_credit",
        "station_no",
        "datestr",
        "timestamp",
        "lat",
        "lon",
        "num_records",
        "shallowest_depth",
        "deepest_depth",
        "bottom_depth",
    ]
    data_lists = {attr: [] for attr in string_attrs}
    for ds in dataset_list:
        for attr in string_attrs:
            if attr in ds:
                data_lists[attr].extend(ds[attr].values)
            elif attr in ds.attrs:
                data_lists[attr].extend([ds.attrs[attr]] * len(ds["datestr"]))
            else:
                data_lists[attr].extend([np.nan] * len(ds["datestr"]))

    ds = xr.Dataset(
        coords=dict(
            timestamp=(["profile"], data_lists["timestamp"]),
            lat=(["profile"], data_lists["lat"]),
            lon=(["profile"], data_lists["lon"]),
        ),
        data_vars=dict(
            **{
                attr: xr.DataArray(data_lists[attr], dims=["profile"])
                for attr in string_attrs
                if attr not in ["timestamp", "lon", "lat"]
            },
        ),
        attrs=dict(
            creation_date=str(dt.datetime.now().strftime("%Y-%m-%d %H:%M")),
        ),
    )

    ds.to_netcdf(merged_file)


def add_new_data_to_merged_file(merged_file, ds_list):
    """
    Add new data to an existing merged file by appending dataset information.

    Args:
        merged_file (str): Path to the merged file.
        ds_list (list): List of datasets to be appended to the merged file.
    """
    if os.path.isfile(merged_file):
        print("adding new data")
        ds_list.append(xr.open_dataset(merged_file))
        create_merged_file(merged_file, ds_list)
    else:
        print("File {} do not exist. Creating it".format(merged_file))
