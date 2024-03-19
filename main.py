from id_creation.assign_id import assign_id_to_profiles
from create_empty_uber_file.uber import create_uber_file
import os
import xarray as xr

ctd_data_path = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/'


# def create_uber():
#     create_uber_file(ctd_data_path)


def create_ncfiles_id():
    assign_id_to_profiles(root_folder='/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/',
                          uber_file=os.getcwd() + "/../uber_file.nc")


if __name__ == '__main__':
    create_ncfiles_id()
    # ds = xr.open_dataset(ctd_data_path+"/DFO_IOS_2022/ncfiles_id/DFO_2022_bottles_RAW_id.nc")
    # print(ds['profile_id'])

    # ds = xr.open_dataset('/home/novaisc/Documents/oceanModelling/data/AW_CAA/uber_file.nc')
    # print(ds['profile_id'])
