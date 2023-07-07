import heapq

import numpy as np
from raster import Raster


def is_valid(proposed_y, proposed_x, max_y, max_x) -> bool:
    """
    Check ik a neighbouring partition exists
    """
    return 0 <= proposed_y <= max_y and 0 <= proposed_x <= max_x


def is_in_bound(location, padding, partition_size) -> bool:
    """
    Check if a location is within bound of the un-padded partition
    """
    return padding <= location[0] <= partition_size and padding <= location[1] <= partition_size


def is_on_bound(location, padding, partition_size) -> bool:
    """
    Check if a location is on the bound of the un-padded partition
    """
    return padding == location[0] or partition_size == location[0] or padding == location[1] or partition_size == location[1]


def is_lower(new_value, old_value) -> bool:
    """
    Check if the proposed ACD is lower that the current stored value
    Function has a threshold to correct for floating point errors
    # TODO find better way to deal with floating point
    """
    return new_value < old_value and np.abs(new_value - old_value) > .05


def get_neighbour_partitions(accumulated_cost_raster: Raster, change) -> [tuple]:
    """
    Transform the num-pad position to the neighbour into the partition location
    """
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
    """
    Creating a heap list with the starting cells for the cost distance calculations
    Source cells will be starting cell, as well as all non-infinite cells on the boundary from neighbouring partitions
    """
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


def get_relative_neighbour_position(location: (int, int), padding, partition_size) -> {int}:
    """
    Returns the num-pad position of the neighbouring partition
    """

    if location[0] == location[1] == padding:
        return {4, 7, 8}
    if location[0] == location[1] == partition_size:
        return {2, 3, 6}
    if location[0] == padding and location[1] == partition_size:
        return {8, 9, 6}
    if location[0] == partition_size and location[1] == padding:
        return {4, 1, 2}
    if not(padding < location[0] < partition_size or padding < location[1] < partition_size):
        return set()
    if location[0] == padding:
        return {8}
    if location[0] == partition_size:
        return {2}
    if location[1] == padding:
        return {4}
    if location[1] == partition_size:
        return {6}
    raise ValueError(f"received non supported tuple {location}")


def calculate_accumulated_cost(cost_raster, local_tuple, calc_tuple, distance, local_cost) -> float:
    """
    Accumulated cost at a cell. Note, this is not the minimum accumulated cost
    """
    return (cost_raster[local_tuple] + cost_raster[calc_tuple]) / 2 * distance + local_cost


def neighbour_generator(neighbour_range) -> (int, int):
    """
    Generator function to loop though the neighbours with one loop
    """
    for i in neighbour_range:
        for j in neighbour_range:
            yield i, j


def do_cost_distance(cost_raster: Raster, accumulated_cost_raster: Raster) -> (Raster, bool):
    """
    Main function to perform cost distance calculations on a raster.
    """
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

    neighbour_range = range(-1, 2)

    active_cells = create_active_cell_dict(accumulated_cost)
    relative_neighbours = set()

    while active_cells:
        local_accumulated_cost, local_tuple = heapq.heappop(active_cells)

        for i, j in neighbour_generator(neighbour_range):
            if i == 0 and j == 0:  # i == 0 & j == 0 is the cell itself
                continue

            neighbour_y, neighbour_x = local_tuple[0] + i, local_tuple[1] + j
            neighbour_tuple = (neighbour_y, neighbour_x)

            if not is_in_bound(neighbour_tuple, padding, partition_size):
                continue

            distance = get_distance(i, j)
            new_value = calculate_accumulated_cost(cost_surface_pad,
                                                   local_tuple,
                                                   neighbour_tuple,
                                                   distance,
                                                   local_accumulated_cost)
            old_value = accumulated_cost[neighbour_tuple]

            if not is_lower(new_value, old_value):
                continue

            accumulated_cost[neighbour_tuple] = new_value
            heapq.heappush(active_cells, (new_value, neighbour_tuple))

            if not is_on_bound(neighbour_tuple, padding, partition_size):
                continue

            neighbours = get_relative_neighbour_position(neighbour_tuple, padding, partition_size)
            relative_neighbours = relative_neighbours.union(neighbours)

    neighbours_to_update = []
    if relative_neighbours:
        neighbours_to_update = get_neighbour_partitions(accumulated_cost_raster, relative_neighbours)

    accumulated_cost_raster.array = accumulated_cost
    return accumulated_cost_raster, neighbours_to_update
