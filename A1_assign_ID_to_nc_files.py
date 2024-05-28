"""
This script generates unique profile IDs for profiles stored in various database folders. It reads NetCDF files
from each database folder, assigns profile IDs to each profile, and saves the updated files with profile IDs 
appended. It also maintains a merged file containing all dataset information (ONLY metadata) with profile IDs.

Steps followed in the script:
1. Initialize the IDGenerator class with the root folder and merged file paths.
2. Iterate through database folders, processing NetCDF files in each.
3. Generate unique profile IDs for each profile in the datasets.
4. Append profile IDs to the datasets and save them as new NetCDF files.
5. Update the merged file with new dataset information.

Functions and methods in the script:
1. IDGenerator.run: Main function to execute the profile ID generation process.
2. IDGenerator.get_profile_id: Generates a unique profile ID.
3. IDGenerator.save_new_id: Saves a new profile ID to a file.
4. IDGenerator.get_all_ids: Retrieves all existing profile IDs from the merged file.
5. IDGenerator.create_id: Creates a random profile ID.
6. IDGenerator.check_duplicates: Checks for duplicate profile IDs.
7. main: Main function to initiate the profile ID generation process.
"""

import os
import random
import xarray as xr
from A1b_create_merged_metadata import create_merged_file, add_new_data_to_merged_file
from tqdm import tqdm
import numpy as np
import string


class IDGenerator:
    """Class to generate unique profile IDs for datasets"""

    def __init__(self, root_folder, merged_file):
        """
        Initialize the IDGenerator class.

        Args:
            root_folder (str): Path to the root folder containing database folders.
            merged_file (str): Path to the merged file containing dataset information.
        """
        self.root_folder = root_folder
        self.merged_file = merged_file
        pass

    def run(self):
        """Main function to execute the profile ID generation process."""
        os.chdir(self.root_folder)
        all_current_id_list = self.get_all_ids(self.merged_file)
        for database_folder in [f.name for f in os.scandir() if f.is_dir() and not f.name.startswith(".")]:
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
                        all_current_id_list, profile_id = self.get_profile_id(all_current_id_list)
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
                print(f"No ncfiles_raw for {database_folder} dataset. Going to next database.")
                continue
            if not os.path.isfile(self.merged_file):
                create_merged_file(self.merged_file, dataset_list)
            else:
                add_new_data_to_merged_file(self.merged_file, dataset_list)
            os.chdir(self.root_folder)

    def get_profile_id(self, id_list):
        """
        Generate a unique profile ID.

        Args:
            id_list (np.array): Array containing existing profile IDs.

        Returns:
            tuple: Updated list of profile IDs and new profile ID.
        """
        profile_id = self.create_id()
        while profile_id in id_list:
            # print(f"profile id duplicated: {profile_id}. Creating another one...")
            profile_id = self.create_id()

        id_list = np.append(id_list, profile_id)
        return id_list, profile_id

    def save_new_id(self, profile_id):
        """
        Save a new profile ID to a file.

        Args:
            profile_id (str): Profile ID to be saved.
        """
        with open("profiles.id", "a") as file:
            file.write(profile_id + "\n")

    def get_all_ids(self, merged_file):
        """
        Retrieve all existing profile IDs from the merged file.

        Args:
            merged_file (str): Path to the merged file.

        Returns:
            np.array: Array containing all existing profile IDs.
        """
        if os.path.isfile(merged_file):
            ds = xr.open_dataset(merged_file)
            id_list = ds["profile_id"].values
        else:
            print("{} does not exist.".format(merged_file))
            id_list = []

        return np.array(id_list)

    def create_id(self):
        """
        Create a random profile ID.

        Returns:
            str: Randomly generated profile ID.
        """
        characters = string.ascii_uppercase + string.digits
        profile_id = "".join(random.choice(string.ascii_uppercase)) + "".join(
            random.choice(characters) for _ in range(4)
        )
        return str(profile_id)

    def check_duplicates(self, id_list):
        """
        Check for duplicate profile IDs.

        Args:
            id_list (np.array): Array containing profile IDs.
        """
        unique_ids, counts = np.unique(id_list, return_counts=True)
        for unique_id, count in zip(unique_ids, counts):
            if count > 1:
                print(f"{unique_id}: {count} occurrences")


def main(root_folder, merged_file):
    """
    Main function to initiate the profile ID generation process.

    Args:
        root_folder (str): Path to the root folder containing database folders.
        merged_file (str): Path to the merged file containing dataset information.
    """
    id_generator = IDGenerator(root_folder, merged_file)
    id_generator.run()


if __name__ == "__main__":
    root = "/mnt/storage6/caio/AW_CAA/CTD_DATA/"
    merged = "/mnt/storage6/caio/AW_CAA/merged_file.nc"
    main(root, merged)
