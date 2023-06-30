import numpy as np


class RasterTemplate:
    def __init__(self, partition_size, padding, rows, cols, cell_size, no_data_value, y_max, x_max):
        self.PARTITION_SIZE = partition_size
        self.PADDING = padding
        self.ROWS = rows
        self.COLS = cols
        self.CELL_SIZE = cell_size
        self.NO_DATA_VALUE = no_data_value
        self.Y_MAX = y_max
        self.X_MAX = x_max


class Raster:
    def __init__(self, raster_template: RasterTemplate, partition, raster_band, verbose=False) -> None:
        self.PARTITION_SIZE = raster_template.PARTITION_SIZE
        self.PADDING = raster_template.PADDING
        self.ROWS = raster_template.ROWS
        self.COLS = raster_template.COLS
        self.CELL_SIZE = raster_template.CELL_SIZE
        self.NO_DATA_VALUE = raster_template.NO_DATA_VALUE
        self.Y_MAX = raster_template.Y_MAX
        self.X_MAX = raster_template.X_MAX

        y, x = partition
        assert type(y) == int and type(x) == int
        y_offset = y * self.PARTITION_SIZE - self.PADDING
        x_offset = x * self.PARTITION_SIZE - self.PADDING

        y_range = self.PARTITION_SIZE + 2 * self.PADDING
        x_range = self.PARTITION_SIZE + 2 * self.PADDING

        # Boundary corrections
        if y_offset < 0:
            y_offset = 0
            y_range = y_range - self.PADDING

        if x_offset < 0:
            x_offset = 0
            x_range = x_range - self.PADDING

        if y_offset + y_range > self.ROWS:
            y_range = self.ROWS - y_offset

        if x_offset + x_range > self.COLS:
            x_range = self.COLS - x_offset

        if verbose:
            print(f"x_offset: {x_offset}, "
                  f"y_offset: {y_offset}, "
                  f"x_range: {x_range}, "
                  f"y_range: {y_range}")

        data = np.abs(raster_band.ReadAsArray(x_offset, y_offset, x_range, y_range))
        data[data == self.NO_DATA_VALUE] = np.inf

        self.partition = partition
        self.array = data
        self.y_offset = y_offset
        self.x_offset = x_offset
        self.y_range = y_range
        self.x_range = x_range

    def to_string(self):
        res = f"partition = {self.partition}\n" \
              f"y_offset = {self.y_offset}\n" \
              f"x_offset = {self.x_offset}\n" \
              f"y_range = {self.y_range}\n" \
              f"x_range = {self.x_range}\n" \
              f"shape = {self.array.shape}"
        return res

    def add_global_boundary_and_padding(self, verbose=False) -> None:
        # data, y_offset, x_offset, y_range, x_range, verbose=False):
        if self.x_offset == 0:
            if verbose:
                print(f"Padding x_offset with {self.PADDING}")
            self.array = np.pad(self.array, [(0, 0), (self.PADDING, 0)],
                                mode="constant",
                                constant_values=np.inf)
        if self.y_offset == 0:
            if verbose:
                print(f"Padding y_offset with {self.PADDING}")
            self.array = np.pad(self.array, [(self.PADDING, 0), (0, 0)],
                                mode="constant",
                                constant_values=np.inf)
        if self.x_range + self.x_offset >= self.COLS:
            if verbose:
                print(f"Padding x_range with {self.PARTITION_SIZE + 2 * self.PADDING - self.x_range}")
            self.array = np.pad(self.array,
                                [(0, 0), (0, self.PARTITION_SIZE + 2 * self.PADDING - self.x_range)],
                                mode="constant",
                                constant_values=np.inf)
        if self.y_range + self.y_offset >= self.ROWS:
            if verbose:
                print(f"Padding y_range with {self.PARTITION_SIZE + 2 * self.PADDING - self.y_range}")
            self.array = np.pad(self.array,
                                [(0, self.PARTITION_SIZE + 2 * self.PADDING - self.y_range), (0, 0)],
                                mode="constant",
                                constant_values=np.inf)

    def remove_global_boundary_and_padding(self) -> None:
        # Global boundary
        if self.partition[0] == self.Y_MAX:
            self.array = self.array[:self.y_range + self.PADDING, :]
        if self.partition[1] == self.X_MAX:
            self.array = self.array[:, :self.x_range + self.PADDING]
        # local padding
        self.array = self.array[self.PADDING:-self.PADDING, self.PADDING:- self.PADDING]
