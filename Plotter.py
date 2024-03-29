import matplotlib.pyplot as plt
import numpy as np
import xarray as xr


class Plotter:
    def __init__(self, ncfile):
        self.ds = xr.open_dataset(ncfile)

    def plot_temp_depth(self):
        temp = self.ds['temp'].values
        depth = self.ds['depth'].values

        plt.plot(temp, depth, '.')
        plt.xlabel('temp')
        plt.ylabel('depth')
        plt.gca().invert_yaxis()
        plt.show()

    def plot_psal_depth(self):
        psal = self.ds['psal'].values
        depth = self.ds['depth'].values

        plt.plot(psal, depth, '.')
        plt.xlabel('psal')
        plt.ylabel('depth')
        plt.gca().invert_yaxis()
        plt.show()

    def plot_lon_lat(self):
        lon = self.ds['lon'].values
        lat = self.ds['lat'].values
        plt.plot(lon, lat)
        plt.xlabel('lon')
        plt.ylabel('lat')
        plt.show()


if __name__ == "__main__":
    data = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/WOD_2022/ncfiles_raw/WOD_2022_raw.nc'
    plotter = Plotter(data)
    plotter.plot_temp_depth()
    plotter.plot_psal_depth()
