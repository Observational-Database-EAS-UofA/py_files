import os
import numpy as np
import xarray as xr
from gsw import p_from_z, SP_from_C
from datetime import datetime


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
                    data = xr.open_dataset(file_name)

                    os.chdir(data_base_path)
                    if 'ncfiles_standard' not in os.listdir():
                        os.mkdir('ncfiles_standard')
                    os.chdir('ncfiles_standard')
                    data.to_netcdf(file_name[:-3] + '_standard.nc')
                    os.chdir(ncfiles_path)
            os.chdir(self.root_folder)


def main(root_folder):
    dataset_standardizer = DatasetStandardizer(root_folder)
    dataset_standardizer.run()


if __name__ == "__main__":
    root = "/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA"
    main(root)
