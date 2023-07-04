import os
import warnings


def user_input(n_processes, cost_raster_file_path, source_raster_file_path, output_file_path) -> None:
    if not n_processes > 1:
        raise ValueError("A minimum of two processes are required")

    if not os.path.exists(cost_raster_file_path):
        raise FileNotFoundError(f"Could not find cost raster '{cost_raster_file_path}'")

    if not os.path.exists(source_raster_file_path):
        raise FileNotFoundError(f"Could not find source raster '{source_raster_file_path}'")

    if os.path.exists(output_file_path):
        warnings.warn(f"Overwritten existing output_raster", category=UserWarning)


def cell_size(geo_transform) -> None:
    if not geo_transform[1] == -geo_transform[5]:
        raise ValueError(f"X and Y pixel size are not equal: {geo_transform[1]} != {-geo_transform[5]}")


def size_vs_partitions(n_processes, partition_indices) -> None:
    if not n_processes <= len(partition_indices):
        raise ValueError(f"More processes than partitions available: {n_processes} > {len(partition_indices)}")
