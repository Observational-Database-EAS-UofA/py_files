"""
This script standardizes datasets by converting date-time to UTC timezone, deriving depth from pressure and vice versa,
and adding additional variables like 'len' and 'sum' to observational variables for further analysis.

Steps followed in the script:
1. Convert date-time to UTC timezone if necessary.
2. Derive depth from pressure and vice versa if one of them is missing.
3. Add additional variables like 'len' and 'sum' to observational variables for further analysis.
4. Save the standardized datasets as NetCDF files.

Functions in the script:
1. convert_timezone: Converts date-time to UTC timezone if necessary.
2. standardize_depth_press: Derives depth from pressure and vice versa if necessary.
3. add_len_sum_obs_variables: Adds 'len' and 'sum' variables to observational variables.
4. save_file: Saves the standardized Dataset as a NetCDF file.
5. run: Main function to execute the standardization process for all datasets.
"""

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
        """
        Initialize the DatasetStandardizer with the root folder containing dataset folders.

        Args:
            root_folder (str): Path to the root folder containing dataset folders.
        """
        self.root_folder = root_folder

    def convert_timezone(self, ds: xr.Dataset):
        """
        Convert date-time to UTC timezone if necessary.

        Args:
            ds (xarray.Dataset): Dataset containing date-time information.

        Returns:
            xarray.Dataset: Dataset with date-time converted to UTC timezone.
        """
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

    def __fill_with_nan(self, ds: xr.Dataset, var1, var2):
        """
        Helper function to fill missing values with NaN for the given variables.

        Args:
            ds (xarray.Dataset): Dataset containing variables.
            var1 (str): Primary variable name.
            var2 (str): Secondary variable name.

        Returns:
            tuple: Arrays of the primary variable, row sizes, and flags.
        """
        var_array = []
        var_flag = []
        var_row_size = ds[f"{var1}_row_size"].values
        base_value = 0
        for idx, size in enumerate(var_row_size):
            if size == 0:
                var2_size = ds[f"{var2}_row_size"].values[idx]
                var_array.extend([np.nan] * var2_size)
                var_row_size[idx] = var2_size
                if f"{var1}_flag" in ds:
                    var_flag.extend([np.nan] * var2_size)
            else:
                var_array.extend(ds[var1].values[base_value : base_value + size])
                if f"{var1}_flag" in ds:
                    var_flag.extend(ds[f"{var1}_flag"].values[base_value : base_value + size])
            base_value += size

        return np.array(var_array), np.array(var_row_size), np.array(var_flag)

    def standardize_depth_press(self, ds: xr.Dataset):
        """
        Derive depth from pressure and vice versa if necessary.

        Args:
            ds (xarray.Dataset): Dataset containing pressure and/or depth information.

        Returns:
            xarray.Dataset: Dataset with standardized pressure and depth variables.
        """
        ## the TEOS 10 toolbox treats depth as negative. Convert the depth to negative, if necessary, before submiting to p_from_z function
        if "press" not in ds and "depth" in ds:
            lat_array = np.repeat(np.array(ds["lat"].values), ds["depth_row_size"].values)
            depth = ds["depth"].values
            if np.sum(depth > 0) > np.sum(depth < 0):
                depth = -depth
            ds["press"] = xr.DataArray(p_from_z(depth, lat_array), dims=["press_obs"])
            ds["press_row_size"] = ds["depth_row_size"]
        elif "depth" not in ds and "press" in ds:
            lat_array = np.repeat(np.array(ds["lat"].values), ds["press_row_size"].values)
            ds["depth"] = xr.DataArray(-z_from_p(ds["press"].values, lat_array), dims=["depth_obs"])
            ds["depth_row_size"] = ds["press_row_size"]
        else:
            depth_array, d_row_size, depth_flag = self.__fill_with_nan(ds, "depth", "press")
            press_array, p_row_size, press_flag = self.__fill_with_nan(ds, "press", "depth")

            nan_depth_indices = np.isnan(depth_array)
            nan_press_indices = np.isnan(press_array)

            lat_array = np.repeat(ds["lat"].values, d_row_size)

            ### fill out all depth and pressure values with convertions. Important to notice that the depth values are beeing converted to positive, since TEOS 10 consider depth negative
            z_values = -z_from_p(press_array, lat_array)
            if np.sum(depth_array > 0) > np.sum(depth_array < 0):
                depth_array = -depth_array
            p_values = p_from_z(depth_array, lat_array)

            ds = ds.drop_vars(["depth", "press"])
            if len(depth_flag) != 0:
                ds = ds.drop_vars("depth_flag")
                ds["depth_flag"] = xr.DataArray(depth_flag, dims=["depth_obs"])
            if len(press_flag) != 0:
                ds = ds.drop_vars("press_flag")
                ds["press_flag"] = xr.DataArray(press_flag, dims=["press_obs"])
            ds["depth"] = xr.where(nan_depth_indices, z_values, xr.DataArray(depth_array, dims=["depth_obs"]))
            ds["press"] = xr.where(nan_press_indices, p_values, xr.DataArray(press_array, dims=["press_obs"]))
            ds["depth_row_size"] = xr.DataArray(d_row_size, dims=["profile"])
            ds["press_row_size"] = xr.DataArray(p_row_size, dims=["profile"])

        return ds

    def add_len_sum_obs_variables(self, ds):
        """
        Add 'len' and 'sum' variables to observational variables.

        Args:
            ds (xarray.Dataset): Dataset containing observational variables.

        Returns:
            xarray.Dataset: Dataset with 'len' and 'sum' variables added to observational variables.
        """
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
        """
        Save the standardized Dataset as a NetCDF file.

        Args:
            ds (xarray.Dataset): Standardized Dataset to be saved.
            database_path (str): Path to the database folder.
            ncfiles_path (str): Path to the ncfiles folder.
            filename (str): Name of the original NetCDF file.
        """
        os.chdir(database_path)
        if "ncfiles_standard" not in os.listdir():
            os.mkdir("ncfiles_standard")
        os.chdir("ncfiles_standard")
        ds.to_netcdf(filename[:-3] + "_standard.nc")
        os.chdir(ncfiles_path)

    # def test_run(self):
    #     os.chdir(self.root_folder)
    #     files = [f.name for f in os.scandir() if f.name.endswith(".nc")]
    #     print(files)
    #     for file_name in files:
    #         print(f"Running on {file_name} file")
    #         ds = xr.open_dataset(file_name)
    #         ds = self.convert_timezone(ds)
    #         ds = self.standardize_depth_press(ds)

    #         ### standardize salinity
    #         if "psal" not in ds:
    #             ds["psal"] = xr.DataArray([np.nan] * len(ds["depth"]), dims=["psal_obs"])

    #         ds = self.add_len_sum_obs_variables(ds)
    #         # print(ds)

    #         ds.close()

    def run(self):
        """Main function to execute the standardization process for all datasets."""
        os.chdir(self.root_folder)
        for database_folder in [f.name for f in os.scandir() if f.is_dir()]:
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
    """
    Main function to initiate the dataset standardization process.

    Args:
        root_folder (str): Path to the root folder containing dataset folders.
    """
    dataset_standardizer = DatasetStandardizer(root_folder)
    dataset_standardizer.run()


if __name__ == "__main__":
    print("Standardazing all data and saving it to ncfiles_standard/ folder inside each database folder...")
    root = "/mnt/storage6/caio/AW_CAA/CTD_DATA"

    start_time = time.time()
    main(root)
    # print(f"total time spent: {time.time() - start_time} seconds")
