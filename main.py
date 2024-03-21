from id_creation.assign_id import assign_id_to_profiles
from unified.unified_file import create_unified_file
import os
import xarray as xr

root_folder = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/'
unified_file = '/home/novaisc/workspace/obs_database/AW_CAA/unified_file.nc'


def create_ncfiles_id():
    assign_id_to_profiles(root_folder=root_folder, unified_file=unified_file)


if __name__ == '__main__':
    create_ncfiles_id()
