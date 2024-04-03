import os
import random
import xarray as xr
from A1c_create_merged_metadata import create_merged_file, add_new_data_to_merged_file
import numpy as np
import string


class IDGenerator:
    def __init__(self, root_folder, merged_file):
        self.root_folder = root_folder
        self.merged_file = merged_file
        pass

    def run(self):
        os.chdir(self.root_folder)
        all_current_id_list = self.get_all_ids(self.merged_file)
        for database_folder in [f.name for f in os.scandir() if f.is_dir() and not f.name.startswith('.')]:
            dataset_list = []
            os.chdir(database_folder)
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
                        all_current_id_list, profile_id = self.get_profile_id(all_current_id_list)
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
            else:
                print(f"No ncfiles_raw for {database_folder} dataset. Going to next database.")
                continue
            if not os.path.isfile(self.merged_file):
                create_merged_file(self.merged_file, dataset_list)
            else:
                add_new_data_to_merged_file(self.merged_file, dataset_list)
            os.chdir(self.root_folder)

    def get_profile_id(self, id_list):
        profile_id = self.create_id()
        while profile_id in id_list:
            print("profile id duplicated: {}".format(profile_id))
            profile_id = self.create_id()

        id_list = np.append(id_list, profile_id)
        return id_list, profile_id

    def save_new_id(self, profile_id):
        with open("profiles.id", "a") as file:
            file.write(profile_id + "\n")

    def get_all_ids(self, merged_file):
        if os.path.isfile(merged_file):
            ds = xr.open_dataset(merged_file)
            id_list = ds['profile_id'].values
        else:
            print("{} does not exist.".format(merged_file))
            id_list = []

        return np.array(id_list)

    def create_id(self):
        characters = string.ascii_uppercase + string.digits
        profile_id = ''.join(random.choice(string.ascii_uppercase)) + ''.join(
            random.choice(characters) for _ in range(4))
        return str(profile_id)

    def check_duplicates(self, id_list):
        unique_ids, counts = np.unique(id_list, return_counts=True)
        for unique_id, count in zip(unique_ids, counts):
            if count > 1:
                print(f"{unique_id}: {count} occurrences")


def main(root_folder, merged_file):
    id_generator = IDGenerator(root_folder, merged_file)
    id_generator.run()


if __name__ == "__main__":
    root = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/'
    merged = '/home/novaisc/workspace/obs_database/AW_CAA/unified_file.nc'
    main(root, merged)
