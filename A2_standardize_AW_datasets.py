import os
import time
import numpy as np
import pandas as pd
import xarray as xr
from datetime import datetime
from gsw import p_from_z, z_from_p
from tqdm import tqdm
import pytz


class DatasetStandardizer:
    def __init__(self, root_folder):
        self.root_folder = root_folder

    ### some datasets may be not using the UTC format. This function converts the datetime to UTC.
    def convert_timezone(self, ds: xr.Dataset):
        if "timezone" in ds and "datestr" in ds:
            datestr = ds["datestr"].values
            timezone = ds["timezone"].values
            mdt_indices = timezone == "MDT"

            if np.any(mdt_indices):
                mdt_tz = pytz.timezone("America/Denver")
                datetime_array = pd.to_datetime(datestr[mdt_indices], format="%Y/%m/%d %H:%M:%S")
                datetime_array = datetime_array.tz_localize(mdt_tz).tz_convert("UTC")
                datestr[mdt_indices] = datetime_array.strftime("%Y/%m/%d %H:%M:%S")

            ds = ds.drop_vars("timezone")
            ds["datestr"] = xr.DataArray(datestr, dims=["profile"], attrs={"timezone": "UTC"})
        return ds

    ### Some datasets has nan values in some measurements, like depth and pressure. So, this function derives depth from pressure and vice versa
    def standardize_depth_press(self, ds: xr.Dataset):
        parent_index = ds["parent_index"].values

        ## lat dimension is "profile". So it's necessary to transform lat to "obs" dimension, so we can operate with it.
        lat_array = np.repeat(np.array(ds["lat"].values), np.unique(parent_index, return_counts=True)[1])

        ## the TEOS 10 toolbox treats depth as negative. Convert the depth to negative, if necessary, before submiting to p_from_z function
        if "press" not in ds and "depth" in ds:
            depth = ds["depth"].values
            if np.sum(depth > 0) > np.sum(depth < 0):
                depth = -depth
            ds["press"] = xr.DataArray(
                p_from_z(depth, lat_array),
                dims=["obs"],
            )
        if "depth" not in ds and "press" in ds:
            ds["depth"] = xr.DataArray(
                z_from_p(ds["press"].values, lat_array),
                dims=["obs"],
            )
            ds["depth"] = -ds["depth"]

        nan_depth_indices = np.isnan(ds["depth"])
        nan_press_indices = np.isnan(ds["press"])

        ### fill out all depth and pressure values with convertions. Important to notice that the depth values are beeing converted to positive, since TEOS 10 consider depth negative
        z_values = -z_from_p(ds["press"].values, lat_array)
        depth = ds["depth"].values
        if np.sum(depth > 0) > np.sum(depth < 0):
            depth = -depth
        p_values = p_from_z(depth, lat_array)

        ds["depth"] = xr.where(nan_depth_indices, z_values, ds["depth"])
        ds["press"] = xr.where(nan_press_indices, p_values, ds["press"])

        return ds

    def add_len_sum_obs_variables(self, ds):
        ### add len and sum to all obs variables to use in the merged file after
        ds["depth"].attrs = {
            **ds["depth"].attrs,
            "lendepth": len(ds["depth"]),
            "sumdepth": np.sum(ds["depth"].values[~np.isnan(ds["depth"].values)]),
        }
        ds["press"].attrs = {
            **ds["press"].attrs,
            "lenpress": len(ds["press"]),
            "sumpress": np.sum(ds["press"].values[~np.isnan(ds["press"].values)]),
        }
        ds["temp"].attrs = {
            **ds["temp"].attrs,
            "lentemp": len(ds["temp"]),
            "sumtemp": np.sum(ds["temp"].values[~np.isnan(ds["temp"].values)]),
        }
        ds["psal"].attrs = {
            **ds["psal"].attrs,
            "lenpsal": len(ds["psal"]),
            "psal": np.sum(ds["psal"].values[~np.isnan(ds["psal"].values)]),
        }
        return ds

    def save_file(self, ds, database_path, ncfiles_path, filename):
        os.chdir(database_path)
        if "ncfiles_standard" not in os.listdir():
            os.mkdir("ncfiles_standard")
        os.chdir("ncfiles_standard")
        ds.to_netcdf(filename[:-3] + "_standard.nc")
        os.chdir(ncfiles_path)

    def run(self):
        os.chdir(self.root_folder)
        for database_folder in [f.name for f in os.scandir() if f.is_dir()]:
            # for database_folder in [f.name for f in os.scandir() if f.is_dir() and f.name.startswith("MEDS_2021")]:
            os.chdir(database_folder)
            data_base_path = os.getcwd()
            if "ncfiles_id" in os.listdir():
                os.chdir("ncfiles_id")
                ncfiles_path = os.getcwd()
                for file_name in [f.name for f in os.scandir() if f.name.endswith(".nc")]:
                    print(f"Running on {file_name} file")
                    ds = xr.open_dataset(file_name)
                    ds = self.convert_timezone(ds)
                    ds = self.standardize_depth_press(ds)

                    ### standardize salinity
                    if "psal" not in ds:
                        ds["psal"] = xr.DataArray([np.nan] * len(ds["depth"]), dims=["obs"])

                    ds = self.add_len_sum_obs_variables(ds)
                    self.save_file(ds, data_base_path, ncfiles_path, file_name)

                    ds.close()
            os.chdir(self.root_folder)


def main(root_folder):
    dataset_standardizer = DatasetStandardizer(root_folder)
    dataset_standardizer.run()


if __name__ == "__main__":
    print("Standardazing all data and saving it to ncfiles_standard/ folder inside each database folder...")
    root = "/mnt/storage6/caio/AW_CAA/CTD_DATA"

    start_time = time.time()
    main(root)
    # print(f"total time spent: {time.time() - start_time} seconds")
