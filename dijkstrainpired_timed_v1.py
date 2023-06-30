import time

import numpy as np
from numba import jit, njit
from tqdm import trange
import CostDistance as cd
from raster import Raster
import os
from osgeo import gdal
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("y", help="y rows",
                    type=int)
parser.add_argument("x", help="x rows",
                    type=int)
parser.add_argument("n_sources", help="number of source cells",
                    type=int)
parser.add_argument("Master_file", help="name of the sourcefile",
                    type=str)
args = parser.parse_args()

PADDING = 1
from time import perf_counter

start_time = time.perf_counter()

N_sources = args.n_sources
y, x = args.y, args.x
Master = args.Master_file
mid_name = f"{Master}_{y*x}-{y}-{x}"
PARTITION_SIZE = max(y, x)
root_folder = os.path.dirname(os.path.realpath(__file__))
size = 1

SourceRasterLocation = root_folder + f'/test_tiff_files/{mid_name}-{N_sources}.tif'
CostRasterLocation = root_folder + f'/test_tiff_files/{mid_name}_cost.tif'

SourceRaster = gdal.Open(SourceRasterLocation, gdal.GA_ReadOnly)
CostRaster = gdal.Open(CostRasterLocation, gdal.GA_ReadOnly)

SourceRasterBand = SourceRaster.GetRasterBand(1)
CostRasterBand = CostRaster.GetRasterBand(1)
NoDataValue = CostRasterBand.GetNoDataValue()
gt = SourceRaster.GetGeoTransform()
PixelSizeX = gt[1]
PixelSizeY = -gt[5]
Cell_size = PixelSizeY

COLS = CostRaster.RasterXSize
ROWS = CostRaster.RasterYSize

CostSurface = Raster(PARTITION_SIZE, PADDING, ROWS, COLS, NoDataValue, (0, 0), raster_band=CostRasterBand)
SourceSurface = Raster(PARTITION_SIZE, PADDING, ROWS, COLS, NoDataValue, (0, 0), raster_band=SourceRasterBand)

CostSurface.array = np.pad(CostSurface.array, [(PADDING, PADDING), (PADDING, PADDING)],
                           mode="constant",
                           constant_values=np.inf)

SourceSurface.array = np.pad(SourceSurface.array, [(PADDING, PADDING), (PADDING, PADDING)],
                             mode="constant",
                             constant_values=np.inf)

cd.do_cost_distance(CostSurface, SourceSurface)

total_time = time.perf_counter() - start_time

with open(f"E:/OneDrive - Universiteit Utrecht/UNI/Master/Thesis/Testing/Code/TestResults_R_22DZ1_weak.csv", "a") as f:
    f.write(f"{y * x},{y},{x},0,{N_sources},{size},{total_time},0\n")