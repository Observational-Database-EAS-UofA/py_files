import os
import numpy as np
import xarray as xr
from gsw import p_from_z, SP_from_C
from datetime import datetime


def standardize_datasets(root_folder):
    os.chdir(root_folder)

    for database_folder in [f.name for f in os.scandir() if f.is_dir() and not f.name.startswith('.')]:
        os.chdir(database_folder)
        data_base_path = os.getcwd()
        if 'ncfiles_id' in os.listdir():
            os.chdir('ncfiles_id')
            ncfiles_path = os.getcwd()
            for file_name in [f.name for f in os.scandir() if f.name.endswith('.nc')]:
                data = xr.open_dataset(file_name)
                if 'dataset' in data:
                    data = data.rename({'dataset': 'dataset_name'})
                if 'dataset' not in data and 'dataset_name' not in data:
                    data = data.assign_attrs(dataset_name=database_folder)
                if 'filename' in data:
                    data = data.rename({'filename': 'orig_filename'})
                if 'orig_filename' not in data:
                    filename_var = xr.DataArray([file_name], name='orig_filename', )
                    data = xr.merge([data, filename_var.to_dataset()])
                if 'timestamp' not in data and 'serialtime' in data:
                    data = data.rename({'serialtime': 'timestamp'})
                if 'timestamp' not in data and 'datestr' in data:
                    serialtime_var = xr.DataArray(
                        [datetime.strptime(item, "%Y/%m/%d %H:%M:%S.%f").timestamp() for item in
                         data['datestr'].values],
                        dims='profile',
                        name='timestamp'
                    )
                    data = xr.merge([data, serialtime_var.to_dataset()], )
                if 'lat' not in data and 'latitude' in data:
                    data = data.rename({'latitude': 'lat'})
                if 'lon' not in data and 'longitude' in data:
                    data = data.rename({'longitude': 'lon'})
                if 'temp' not in data and 'temperature' in data:
                    data = data.rename({'temperature': 'temp'})
                if 'press' not in data and 'pressure' in data:
                    data = data.rename({'pressure': 'press'})
                if 'press' not in data and 'depth':
                    if 'MEDS_2021' in database_folder and 'P':
                        print("to do")

                if 'press' not in data and 'z' in data:
                    press_var = [p_from_z(data['z'].values[i] * -1, data['lat'].values[i]) for i in
                                 range(len(data['z']))]
                    data['press'] = xr.DataArray(np.concatenate(press_var), dims=['profile'], name='press')
                if 'press' not in data and 'depth' in data:
                    press_var = [p_from_z(data['depth'].values[i] * -1, data['lat'].values[i]) for i in
                                 range(len(data['depth']))]
                    data['press'] = xr.DataArray(np.concatenate(press_var), dims=['profile'], name='press')
                if 'psal' not in data and 'SP' in data:
                    data = data.rename({'SP': 'sal'})
                if 'psal' not in data and 'sal' in data:
                    data = data.rename({'sal': 'psal'})
                if 'psal' not in data and 'salinity' in data:
                    data = data.rename({'salinity': 'psal'})

                if 'psal' in data and np.isnan(data['psal']).all():
                    data['psal'] = np.nan * np.arange(len(data['temp']))
                if 'temp' in data and np.isnan(data['temp']).all():
                    data['temp'] = np.nan * np.arange(len(data['psal']))
                if 'psal' not in data and 'SP' not in data and 'conductivity' in data:
                    data['psal'] = SP_from_C(data['conductivity'], data['temp'], data['press'])
                if 'psal' not in data and 'sal' not in data and 'PS' not in data:
                    data['psal'] = np.nan * np.arange(len(data['temp']))

                os.chdir(data_base_path)
                if 'ncfiles_standard' not in os.listdir():
                    os.mkdir('ncfiles_standard')
                os.chdir('ncfiles_standard')
                data.to_netcdf(file_name[:-3] + '_standard.nc')
                os.chdir(ncfiles_path)
        os.chdir(root_folder)
