import os
import string
import random
import xarray as xr

import numpy as np


def get_profile_id(id_list):
    # current_dir = os.getcwd()
    # os.chdir("/home/novaisc/Documents/oceanModelling/data/AW_CAA/")
    profile_id = create_id()
    while profile_id in id_list:
        print("profile id duplicated: {}".format(profile_id))
        profile_id = create_id()

    # save_new_id(profile_id)
    # os.chdir(current_dir)
    return profile_id


def save_new_id(profile_id):
    with open("profiles.id", "a") as file:
        file.write(profile_id + "\n")


def get_all_ids(uber_file):
    try:
        ds = xr.open_dataset(uber_file)
        id_list = ds['profile_id'].values
    except Exception as e:
        print(f"An error occurred: {e}")
        id_list = []

    return np.array(id_list)


def create_id():
    characters = string.ascii_uppercase + string.digits
    profile_id = ''.join(random.choice(string.ascii_uppercase)) + ''.join(
        random.choice(characters) for _ in range(4))
    return profile_id


def check_duplicates(id_list):
    unique_ids, counts = np.unique(id_list, return_counts=True)
    for unique_id, count in zip(unique_ids, counts):
        if count > 1:
            print(f"{unique_id}: {count} occurrences")
