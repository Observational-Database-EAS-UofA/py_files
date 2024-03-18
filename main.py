from id_creation.assign_id import assign_id_to_profiles
from create_empty_uber_file.uber import create_uber_file
import os

ctd_data_path = '/home/novaisc/Documents/oceanModelling/data/AW_CAA/CTD_DATA/'


def create_uber():
    create_uber_file(ctd_data_path)


def create_ncfiles_id():
    assign_id_to_profiles(uber_file=os.getcwd() + "/../uber_file.nc")


if __name__ == '__main__':
    # create_ncfiles_id()
    create_uber()
