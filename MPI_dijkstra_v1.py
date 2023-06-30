import time
import argparse

from mpi4py import MPI
from osgeo import gdal
import numpy as np
from mpi_print import print

import init
import checks
from CostDistance import do_cost_distance
from raster import RasterTemplate, Raster

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--cost", help="Cost raster file", type=str, default="demo/demo_cost.tif")
parser.add_argument("-s", "--source", help="Source raster file", type=str, default="demo/demo_source.tif")
parser.add_argument("-o", "--output", help="Output file", type=str, default="output/output.tif")
parser.add_argument("-p", "--partition_size", help="Partition size", type=int, default=250)
parser.add_argument("-t", "--time", help="Time the root node", action='store_true')
args = parser.parse_args()
print(args)

COMM = MPI.COMM_WORLD
rank = COMM.Get_rank()
size = COMM.Get_size()

checks.user_input(size, args.cost, args.source, args.output)

PARTITION_SIZE = args.partition_size
PADDING = 1  # If later implementations use Knight move, change this

ROOT_ID = 0

SourceRasterLocation = args.source
SourceRaster = gdal.Open(SourceRasterLocation, gdal.GA_ReadOnly)
SourceRasterBand = SourceRaster.GetRasterBand(1)

CostRasterLocation = args.cost
CostRaster = gdal.Open(CostRasterLocation, gdal.GA_ReadOnly)
CostRasterBand = CostRaster.GetRasterBand(1)

CumulativeCostRasterLocation = args.output
NO_DATA_VALUE = CostRasterBand.GetNoDataValue()

checks.cell_size(SourceRaster.GetGeoTransform())

CELL_SIZE = SourceRaster.GetGeoTransform()[1]

COLS = CostRaster.RasterXSize
ROWS = CostRaster.RasterYSize

Y_PARTITIONS = np.arange(int(np.ceil(ROWS / PARTITION_SIZE)))
X_PARTITIONS = np.arange(int(np.ceil(COLS / PARTITION_SIZE)))
Y_MAX = max(Y_PARTITIONS)
X_MAX = max(X_PARTITIONS)

PARTITION_INDICES = [(y, x) for y in Y_PARTITIONS for x in X_PARTITIONS]
checks.size_vs_partitions(size, PARTITION_INDICES)

RASTER_TEMPLATE = RasterTemplate(PARTITION_SIZE, PADDING, ROWS, COLS, CELL_SIZE, NO_DATA_VALUE, Y_MAX, X_MAX)

if rank == ROOT_ID:
    def overwrite_raster(data, partition_to_save, raster_band):
        partition_y, partition_x = partition_to_save
        y_offset = partition_y * PARTITION_SIZE
        x_offset = partition_x * PARTITION_SIZE
        data[data == np.inf] = NO_DATA_VALUE
        raster_band.WriteArray(data, x_offset, y_offset)
        raster_band.FlushCache()


    def stop_all_workers(number_of_processes) -> None:
        for i in range(1, number_of_processes):
            COMM.send(((-1, -1), []), dest=i)


    start_time = time.perf_counter()

    # Create the accumulated cost raster
    # TODO get this in a function without breaking MPI pickling
    driver = gdal.GetDriverByName('GTiff')
    output_dataset = driver.Create(CumulativeCostRasterLocation, COLS, ROWS, 1, gdal.GDT_Float32)
    output_dataset.SetGeoTransform(CostRaster.GetGeoTransform())
    output_dataset.SetProjection(CostRaster.GetProjection())
    CumulativeCostRasterBand = output_dataset.GetRasterBand(1)
    CumulativeCostRasterBand.WriteArray(np.full((ROWS, COLS), np.inf))
    if NO_DATA_VALUE is not None:
        CumulativeCostRasterBand.SetNoDataValue(NO_DATA_VALUE)

    partitions_to_check = init.partition_set(size, comm=COMM)
    worker_queue = init.worker_queue(size)

    partitions_working_on = set()

    comm_send_timer = 0

    while True:
        if worker_queue.full() and len(partitions_to_check) == 0:
            stop_all_workers(size)
            break

        while not worker_queue.empty():
            # not that following is a set operation
            valid_partitions_to_work_on = partitions_to_check - partitions_working_on

            if not valid_partitions_to_work_on:
                break

            worker_to_send_to = worker_queue.get()
            partition_to_send = valid_partitions_to_work_on.pop()
            partitions_to_check.remove(partition_to_send)
            partitions_working_on.add(partition_to_send)

            cumulativeRaster = Raster(RASTER_TEMPLATE, partition_to_send, CumulativeCostRasterBand)

            comm_send_timer_start = time.perf_counter()
            COMM.send((partition_to_send, cumulativeRaster), dest=worker_to_send_to)
            comm_send_timer += time.perf_counter() - comm_send_timer_start

        worker_rank, worker_partition, new_partitions, array = COMM.recv()

        worker_queue.put(worker_rank)
        partitions_working_on.remove(worker_partition)
        for partition in new_partitions:
            partitions_to_check.add(partition)

        overwrite_raster(array, worker_partition, CumulativeCostRasterBand)

    total_time = time.perf_counter() - start_time
    if args.time:
        with open(f"E:/OneDrive - Universiteit Utrecht/UNI/Master/Thesis/Testing/Code/TestResults.csv", "a") as f:
            f.write(f"{ROWS*COLS},{ROWS},{COLS},{PARTITION_SIZE}, {size},{total_time}, {comm_send_timer}\n")
    print(f"Finished with the cost distance calculation in {total_time} seconds.")


if rank != ROOT_ID:
    """chose which cells to check for init"""
    n_partitions = len(PARTITION_INDICES)
    n_workers = size - 1  # 1 handler
    n_cells_to_handle = int(np.ceil(n_partitions / n_workers))

    start_index = (rank - 1) * n_cells_to_handle
    end_index = min(len(PARTITION_INDICES) - 1, rank * n_cells_to_handle)
    partitions_to_start = PARTITION_INDICES[start_index:end_index]

    partitions_with_source = []
    for y_partition, x_partition in partitions_to_start:
        source_raster = Raster(RASTER_TEMPLATE, (int(y_partition), int(x_partition)), SourceRasterBand)
        if np.any(source_raster.array == 1):
            partitions_with_source.append((int(y_partition), int(x_partition)))

    COMM.send(obj=partitions_with_source, dest=ROOT_ID)

    # Go into the main loop.
    while True:
        partition: (int, int)
        AccumulatedCostRaster: Raster

        partition, AccumulatedCostRaster = COMM.recv(source=ROOT_ID)

        # Break loop when partition (-1, -1) is send.
        if partition == (-1, -1):
            break

        sourceRaster = Raster(RASTER_TEMPLATE, partition, SourceRasterBand)
        costRaster = Raster(RASTER_TEMPLATE, partition, CostRasterBand)

        # accumulated cost is zero when at a source
        AccumulatedCostRaster.array[np.where(sourceRaster.array == 1)] = 0

        costRaster.add_global_boundary_and_padding()
        AccumulatedCostRaster.add_global_boundary_and_padding()

        AccumulatedCostRaster, neighbours_to_update = do_cost_distance(costRaster, AccumulatedCostRaster)

        AccumulatedCostRaster.remove_global_boundary_and_padding()

        COMM.send((rank, partition, neighbours_to_update, AccumulatedCostRaster.array), dest=ROOT_ID)
