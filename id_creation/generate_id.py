import os
import string
import random
import xarray as xr

import numpy as np


def get_profile_id(id_list):
    profile_id = create_id()
    while profile_id in id_list:
        print("profile id duplicated: {}".format(profile_id))
        profile_id = create_id()

    id_list = np.append(id_list, profile_id)
    return id_list, profile_id


def save_new_id(profile_id):
    with open("profiles.id", "a") as file:
        file.write(profile_id + "\n")


def get_all_ids(uber_file):
    if os.path.isfile(uber_file):
        ds = xr.open_dataset(uber_file)
        id_list = ds['profile_id'].values
    else:
        print("{} does not exist.".format(uber_file))
        id_list = []

    return np.array(id_list)


def create_id():
    characters = string.ascii_uppercase + string.digits
    profile_id = ''.join(random.choice(string.ascii_uppercase)) + ''.join(
        random.choice(characters) for _ in range(4))
    return str(profile_id)


def check_duplicates(id_list):
    unique_ids, counts = np.unique(id_list, return_counts=True)
    for unique_id, count in zip(unique_ids, counts):
        if count > 1:
            print(f"{unique_id}: {count} occurrences")
