import os
import numpy as np
import xarray as xr
from datetime import datetime
from gsw import p_from_z, z_from_p
import pytz


class DatasetStandardizer:
    def __init__(self, root_folder):
        self.root_folder = root_folder
        pass

    def run(self):
        os.chdir(self.root_folder)
        for database_folder in [f.name for f in os.scandir() if f.is_dir() and not f.name.startswith('.')]:
            os.chdir(database_folder)
            data_base_path = os.getcwd()
            if 'ncfiles_id' in os.listdir():
                os.chdir('ncfiles_id')
                ncfiles_path = os.getcwd()
                for file_name in [f.name for f in os.scandir() if f.name.endswith('.nc')]:
                    print(file_name)
                    ds = xr.open_dataset(file_name)
                    # convert timezone MDT and GMT to UTC and drop timezone variable
                    if 'timezone' in ds and 'datestr' in ds:
                        datestr = ds['datestr'].values
                        timezone = ds['timezone'].values
                        for i in range(len(datestr)):
                            if timezone[i] == 'MDT':
                                mdt_tz = pytz.timezone('America/Denver')
                                mdt_datetime = datetime.strptime(
                                    datestr[i], '%Y/%m/%d %H:%M:%S')
                                mdt_datetime = mdt_tz.localize(mdt_datetime)
                                utc_datetime = mdt_datetime.astimezone(
                                    pytz.utc)
                                datestr[i] = utc_datetime.strftime(
                                    '%Y/%m/%d %H:%M:%S')
                        ds = ds.drop_vars("timezone")
                        ds['datestr'] = xr.DataArray(
                            datestr, dims=['profile'], attrs={'timezone': 'UTC'})

                    if 'press' not in ds and 'depth' in ds:
                        press_var = []
                        parent_index = ds['parent_index'].values
                        for i,d in enumerate(ds['depth'].values):
                            press_var.append(p_from_z(d * -1, ds['lat'].values[parent_index[i]]))
                        ds['press'] = xr.DataArray(press_var, dims=['obs'],)
                    if 'depth' not in ds and 'press' in ds:
                        depth_var = []
                        parent_index = ds['parent_index'].values
                        for i,p in enumerate(ds['press'].values):
                            press_var.append(p_from_z(p * -1, ds['lat'].values[parent_index[i]]))
                        ds['depth'] = xr.DataArray(depth_var, dims=['obs'],)
                    if 'psal' not in ds:
                        ds['psal'] = xr.DataArray([np.nan] * len(ds['depth']), dims=['obs'])

                    ### add len and sum to all obs variables to use in the merged file after
                    ds['depth'].attrs = {
                        **ds['depth'].attrs, "lendepth": len(ds['depth']), "sumdepth": np.sum(ds['depth'].values[~np.isnan(ds['depth'].values)])}
                    ds['press'].attrs = {
                        **ds['press'].attrs, "lenpress": len(ds['press']), "sumpress": np.sum(ds['press'].values[~np.isnan(ds['press'].values)])}
                    ds['temp'].attrs = {
                        **ds['temp'].attrs, "lentemp": len(ds['temp']), "sumtemp": np.sum(ds['temp'].values[~np.isnan(ds['temp'].values)])}
                    ds['psal'].attrs = {
                        **ds['psal'].attrs, "lenpsal": len(ds['psal']), "psal": np.sum(ds['psal'].values[~np.isnan(ds['psal'].values)])}

                    os.chdir(data_base_path)
                    if 'ncfiles_standard' not in os.listdir():
                        os.mkdir('ncfiles_standard')
                    os.chdir('ncfiles_standard')
                    ds.to_netcdf(file_name[:-3] + '_standard.nc')
                    os.chdir(ncfiles_path)
            os.chdir(self.root_folder)


def main(root_folder):
    dataset_standardizer = DatasetStandardizer(root_folder)
    dataset_standardizer.run()


if __name__ == "__main__":
    root = "/mnt/storage6/caio/AW_CAA/CTD_DATA"
    main(root)
