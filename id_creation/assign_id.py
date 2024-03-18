import os
import xarray as xr
from id_creation.generate_id import get_all_ids, get_profile_id


def assign_id_to_profiles(uber_file):
    rootf = '/home/novaisc/Documents/oceanModelling/data/AW_CAA/CTD_DATA/'
    os.chdir(rootf)
    dataset_list = []
    id_list = get_all_ids(uber_file)
    for database_folder in [f.name for f in os.scandir() if f.is_dir() and not f.name.startswith('.')]:
        os.chdir(database_folder)
        data_base_path = os.getcwd()
        # enter ncfiles
        if 'ncfiles_raw' in os.listdir():
            os.chdir('ncfiles_raw')
            ncfiles_path = os.getcwd()
            for file_name in [f.name for f in os.scandir() if f.name.endswith('.nc')]:
                print(file_name)
                ds = xr.open_dataset(file_name)
                if 'profile_id' in ds or 'profile_ID' in ds:
                    raise ValueError("Profile ID already exists")
                profiles_id = [get_profile_id(id_list) for _ in ds['profile'].values]
                ds['profile_id'] = xr.DataArray(profiles_id, dims=['profile'])
                dataset_list.append(ds)

                # save the file
                os.chdir(data_base_path)
                if 'ncfiles_id' not in os.listdir():
                    os.mkdir('ncfiles_id')
                os.chdir('ncfiles_id')
                ds.to_netcdf(file_name[:-3] + '_id.nc')
                os.chdir(ncfiles_path)
        os.chdir(rootf)
