import heapq

import numpy as np
from raster import Raster


def is_valid(proposed_y, proposed_x, max_y, max_x) -> bool:
    return 0 <= proposed_y <= max_y and 0 <= proposed_x <= max_x


def is_on_partition_boundary(calc_y, calc_x, padding, partition_size) -> bool:
    return padding == calc_y or partition_size == calc_y or padding == calc_x or partition_size == calc_x


def get_neighbour_partitions(accumulated_cost_raster: Raster, change) -> [tuple]:
    current_y, current_x = accumulated_cost_raster.partition
    max_y = accumulated_cost_raster.Y_MAX
    max_x = accumulated_cost_raster.X_MAX
    neighbour_list = set()

    for relative_neighbour in change:

        if relative_neighbour == 1:
            if is_valid(current_y+1, current_x-1, max_y, max_x):
                neighbour_list.add((current_y + 1, current_x-1))
                continue
        if relative_neighbour == 2:
            if is_valid(current_y+1, current_x, max_y, max_x):
                neighbour_list.add((current_y + 1, current_x))
                continue
        if relative_neighbour == 3:
            if is_valid(current_y+1, current_x+1, max_y, max_x):
                neighbour_list.add((current_y + 1, current_x+1))
                continue
        if relative_neighbour == 4:
            if is_valid(current_y, current_x-1, max_y, max_x):
                neighbour_list.add((current_y, current_x-1))
                continue
        if relative_neighbour == 6:
            if is_valid(current_y, current_x+1, max_y, max_x):
                neighbour_list.add((current_y, current_x+1))
                continue
        if relative_neighbour == 7:
            if is_valid(current_y-1, current_x-1, max_y, max_x):
                neighbour_list.add((current_y-1, current_x-1))
                continue
        if relative_neighbour == 8:
            if is_valid(current_y-1, current_x, max_y, max_x):
                neighbour_list.add((current_y-1, current_x))
                continue
        if relative_neighbour == 9:
            if is_valid(current_y-1, current_x+1, max_y, max_x):
                neighbour_list.add((current_y-1, current_x+1))
                continue

    return list(neighbour_list)


def create_active_cell_dict(accumulated_cost) -> []:
    active_cells = []
    heapq.heapify(active_cells)
    known = (np.where(accumulated_cost == 0))
    for i, j in zip(known[0], known[1]):
        heapq.heappush(active_cells, (accumulated_cost[i, j], (i, j)))

    accumulated_cost_copy = accumulated_cost.copy()
    accumulated_cost_copy[1:-1, 1:-1] = np.inf
    known = (np.where(accumulated_cost_copy != np.inf))
    for i, j in zip(known[0], known[1]):
        heapq.heappush(active_cells, (accumulated_cost[i, j], (i, j)))
    return active_cells


def get_relative_neighbour_position(current_tuple: (int, int), padding, partition_size) -> {int}:
    """
    Returns the num-pad position of the neighbouring partition
    """
    calc_y, calc_x = current_tuple

    if calc_y == calc_x == padding:
        return {4, 7, 8}
    if calc_y == calc_x == partition_size:
        return {2, 3, 6}
    if calc_y == padding and calc_x == partition_size:
        return {8, 9, 6}
    if calc_y == partition_size and calc_x == padding:
        return {4, 1, 2}
    if not(padding < calc_y < partition_size or padding < calc_x < partition_size):
        return set()
    if calc_y == padding:
        return {8}
    if calc_y == partition_size:
        return {2}
    if calc_x == padding:
        return {4}
    if calc_x == partition_size:
        return {6}
    raise ValueError(f"received non supported tuple {current_tuple}")


def get_accumulated_cost(cost_raster, local_tuple, calc_tuple, distance, local_cost) -> float:
    return (cost_raster[local_tuple] + cost_raster[calc_tuple]) / 2 * distance + local_cost


def do_cost_distance(cost_raster: Raster, accumulated_cost_raster: Raster) -> (Raster, bool):

    cell_size = cost_raster.CELL_SIZE
    diagonal = np.sqrt(2*cell_size**2)

    def get_distance(y, x):
        if y == 0 or x == 0:
            return cell_size
        else:
            return diagonal

    cost_surface_pad = cost_raster.array
    accumulated_cost = accumulated_cost_raster.array
    padding = cost_raster.PADDING
    partition_size = cost_raster.PARTITION_SIZE
    relative_neighbours = set()
    neighbour_range = range(-1, 2)
    active_cells = create_active_cell_dict(accumulated_cost)

    while active_cells:
        local_value, local_tuple = heapq.heappop(active_cells)

        for i in neighbour_range:
            for j in neighbour_range:
                if not (i == 0 and j == 0):
                    calc_y, calc_x = local_tuple[0] + i, local_tuple[1] + j
                    calc_tuple = (calc_y, calc_x)
                    if 0 < calc_y <= partition_size and 0 < calc_tuple[1] <= partition_size:
                        distance = get_distance(i, j)
                        new_value = get_accumulated_cost(cost_surface_pad, local_tuple, calc_tuple, distance, local_value)
                        old_value = accumulated_cost[calc_tuple]

                        # TODO find better way to correct for floating point problems
                        if new_value < old_value and np.abs(new_value - old_value) > .2:
                            accumulated_cost[calc_tuple] = min(accumulated_cost[calc_tuple], new_value)
                            heapq.heappush(active_cells, (new_value, calc_tuple))
                            if is_on_partition_boundary(calc_y, calc_x, padding, partition_size):
                                neighbours = get_relative_neighbour_position(calc_tuple, padding, partition_size)
                                relative_neighbours = relative_neighbours.union(neighbours)

    neighbours_to_update = []
    if relative_neighbours:
        neighbours_to_update = get_neighbour_partitions(accumulated_cost_raster, relative_neighbours)

    accumulated_cost_raster.array = accumulated_cost
    return accumulated_cost_raster, neighbours_to_update
