import os
import time
import numpy as np
import pandas as pd
import xarray as xr
from datetime import datetime
from gsw import p_from_z, z_from_p
from tqdm import tqdm
import math
import pytz


class DatasetStandardizer:
    def __init__(self, root_folder):
        self.root_folder = root_folder
        pass

    # def convert_timezone(self, ds: xr.Dataset):
    #     # convert timezone MDT and GMT to UTC and drop timezone variable
    #     if "timezone" in ds and "datestr" in ds:
    #         datestr = ds["datestr"].values
    #         timezone = ds["timezone"].values
    #         for i in range(len(datestr)):
    #             if timezone[i] == "MDT":
    #                 mdt_tz = pytz.timezone("America/Denver")
    #                 mdt_datetime = datetime.strptime(datestr[i], "%Y/%m/%d %H:%M:%S")
    #                 mdt_datetime = mdt_tz.localize(mdt_datetime)
    #                 utc_datetime = mdt_datetime.astimezone(pytz.utc)
    #                 datestr[i] = utc_datetime.strftime("%Y/%m/%d %H:%M:%S")
    #         ds = ds.drop_vars("timezone")
    #         ds["datestr"] = xr.DataArray(datestr, dims=["profile"], attrs={"timezone": "UTC"})
    #     return ds

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

    def standardize_depth_press(self, ds: xr.Dataset):
        parent_index = ds["parent_index"].values

        lat_array = np.repeat(np.array(ds["lat"].values), np.unique(parent_index, return_counts=True)[1])

        if "press" not in ds and "depth" in ds:
            # press_var = []
            # for i, d in enumerate(ds["depth"].values):
            #     press_var.append(np.abs(p_from_z(d, ds["lat"].values[parent_index[i]])))
            ds["press"] = xr.DataArray(
                p_from_z(ds["depth"].values, lat_array),
                dims=["obs"],
            )
        if "depth" not in ds and "press" in ds:
            # depth_var = []
            # for i, p in enumerate(ds["press"].values):
            #     press_var.append(np.abs(z_from_p(p, ds["lat"].values[parent_index[i]])))
            ds["depth"] = xr.DataArray(
                z_from_p(ds["press"].values, lat_array),
                dims=["obs"],
            )

        nan_depth_indices = np.isnan(ds["depth"])
        nan_press_indices = np.isnan(ds["press"])

        # n1 = -10
        # n2 = -2
        # print(np.where(nan_press_indices == True))
        # print("depth before: " + "-" *100)
        # print(f"depth: {ds["depth"].values[n1:n2]}")

        # print(f"press: {ds["press"].values[n1:n2]}")
        # print(f"lat: {lat_array[n1:n2]}")
        # print(nan_depth_indices[n1:n2])

        z_values = np.abs(z_from_p(ds["press"].values, lat_array))
        p_values = np.abs(p_from_z(ds["depth"].values, lat_array))

        ds["depth"] = xr.where(nan_depth_indices, z_values, ds["depth"])
        ds["press"] = xr.where(nan_press_indices, p_values, ds["press"])
        # print("depth after" + "-" * 100)
        # print(ds["depth"].values[n1:n2])

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
        for database_folder in [f.name for f in os.scandir() if f.is_dir() and not f.name.startswith(".")]:
            os.chdir(database_folder)
            data_base_path = os.getcwd()
            if "ncfiles_id" in os.listdir():
                os.chdir("ncfiles_id")
                ncfiles_path = os.getcwd()
                for file_name in [f.name for f in os.scandir() if f.name.endswith(".nc")]:
                    print(file_name)
                    ds = xr.open_dataset(file_name)
                    ds = self.convert_timezone(ds)
                    ds = self.standardize_depth_press(ds)

                    ### standardize salinity
                    if "psal" not in ds:
                        ds["psal"] = xr.DataArray([np.nan] * len(ds["depth"]), dims=["obs"])

                    ds = self.add_len_sum_obs_variables(ds)
                    # data_base_path = self.root_folder
                    self.save_file(ds, data_base_path, ncfiles_path, file_name)

                    ds.close()
                    # break

            os.chdir(self.root_folder)


def main(root_folder):
    dataset_standardizer = DatasetStandardizer(root_folder)
    dataset_standardizer.run()


if __name__ == "__main__":
    root = "/mnt/storage6/caio/AW_CAA/CTD_DATA"
    # root = "/mnt/storage6/caio/AW_CAA/CTD_DATA/MEDS_2021/"

    start_time = time.time()
    main(root)
    print(f"total: {time.time() - start_time} seconds")
