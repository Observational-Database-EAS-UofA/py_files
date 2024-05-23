"""
This code processes datasets within a specified root folder, assigns unique profile IDs to each dataset,
and updates a merged file with ONLY the metadata of every dataset. The key operations include:
1. Generating unique profile IDs for each profile in the datasets.
2. Checking for and preventing duplicate profile IDs.
3. Saving the updated datasets with the new profile IDs.
4. Updating a merged file with the metadata for each profile.

The script follows these steps:
1. Initialize the IDGenerator with the root folder containing the data and the path to the merged file.
2. Retrieve all existing profile IDs from the merged file.
3. Iterate through each database folder within the root folder.
4. For each NetCDF file in the 'ncfiles_raw' subdirectory of each database folder:
   a. Open the dataset.
   b. Generate and assign a unique profile ID to each profile.
   c. Save the updated dataset with the new profile IDs to the 'ncfiles_id' subdirectory.
5. Update the merged file.
6. Repeat for all database folders.
"""

import os
import random
import xarray as xr
from A1b_create_merged_metadata import create_merged_file, add_new_data_to_merged_file
from tqdm import tqdm
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
        for database_folder in [
            f.name for f in os.scandir() if f.is_dir() and not f.name.startswith(".")
        ]:
            print(f"running {database_folder} database...")
            dataset_list = []
            os.chdir(database_folder)
            data_base_path = os.getcwd()
            # enter ncfiles
            if "ncfiles_raw" in os.listdir():
                os.chdir("ncfiles_raw")
                ncfiles_path = os.getcwd()
                for file_name in tqdm(
                    [f.name for f in os.scandir() if f.name.endswith(".nc")],
                    colour="GREEN",
                ):
                    ds = xr.open_dataset(file_name)
                    if "profile_id" in ds or "profile_ID" in ds:
                        raise ValueError("Profile ID already exists")
                    profiles_id = []
                    for _ in ds["profile"].values:
                        all_current_id_list, profile_id = self.get_profile_id(
                            all_current_id_list
                        )
                        profiles_id.append(profile_id)

                    ds["profile_id"] = xr.DataArray(
                        profiles_id,
                        dims=["profile"],
                    )
                    dataset_list.append(ds)

                    # save the file
                    os.chdir(data_base_path)
                    if "ncfiles_id" not in os.listdir():
                        os.mkdir("ncfiles_id")
                    os.chdir("ncfiles_id")
                    ds.to_netcdf(file_name[:-3] + "_id.nc")
                    os.chdir(ncfiles_path)
            else:
                print(
                    f"No ncfiles_raw for {database_folder} dataset. Going to next database."
                )
                continue
            if not os.path.isfile(self.merged_file):
                create_merged_file(self.merged_file, dataset_list)
            else:
                add_new_data_to_merged_file(self.merged_file, dataset_list)
            os.chdir(self.root_folder)

    def get_profile_id(self, id_list):
        profile_id = self.create_id()
        while profile_id in id_list:
            # print(f"profile id duplicated: {profile_id}. Creating another one...")
            profile_id = self.create_id()

        id_list = np.append(id_list, profile_id)
        return id_list, profile_id

    def save_new_id(self, profile_id):
        with open("profiles.id", "a") as file:
            file.write(profile_id + "\n")

    def get_all_ids(self, merged_file):
        if os.path.isfile(merged_file):
            ds = xr.open_dataset(merged_file)
            id_list = ds["profile_id"].values
        else:
            print("{} does not exist.".format(merged_file))
            id_list = []

        return np.array(id_list)

    def create_id(self):
        characters = string.ascii_uppercase + string.digits
        profile_id = "".join(random.choice(string.ascii_uppercase)) + "".join(
            random.choice(characters) for _ in range(4)
        )
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
    root = "/mnt/storage6/caio/AW_CAA/CTD_DATA/"
    merged = "/mnt/storage6/caio/AW_CAA/merged_file.nc"
    main(root, merged)
