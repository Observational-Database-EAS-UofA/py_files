import os
import xarray as xr
from id_creation.generate_id import get_all_ids, get_profile_id
from create_empty_uber_file.uber import create_uber_file, add_new_data_to_uber_file
import numpy as np


def assign_id_to_profiles(root_folder, uber_file):
    os.chdir(root_folder)
    id_list = get_all_ids(uber_file)
    for database_folder in [f.name for f in os.scandir() if f.is_dir() and not f.name.startswith('.')]:
        dataset_list = []
        os.chdir(database_folder)
        os.chdir('python_processing')
        data_base_path = os.getcwd()
        # enter ncfiles
        if 'ncfiles_raw' in os.listdir():
            os.chdir('ncfiles_raw')
            ncfiles_path = os.getcwd()
            for file_name in [f.name for f in os.scandir() if f.name.endswith('.nc')]:
                ds = xr.open_dataset(file_name)
                if 'profile_id' in ds or 'profile_ID' in ds:
                    raise ValueError("Profile ID already exists")
                profiles_id = []
                for _ in ds['profile'].values:
                    id_list, profile_id = get_profile_id(id_list)
                    profiles_id.append(profile_id)

                ds['profile_id'] = xr.DataArray(profiles_id, dims=['profile'], )
                dataset_list.append(ds)

                # save the file
                os.chdir(data_base_path)
                if 'ncfiles_id' not in os.listdir():
                    os.mkdir('ncfiles_id')
                os.chdir('ncfiles_id')
                ds.to_netcdf(file_name[:-3] + '_id.nc')
                os.chdir(ncfiles_path)
        if not os.path.isfile(uber_file):
            create_uber_file(uber_file, dataset_list)
        else:
            add_new_data_to_uber_file(uber_file, dataset_list)
        os.chdir(root_folder)
