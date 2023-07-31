# Parallel cost distance algorithm

A commandline parallel algorithm to do cost cost distance analysis.

This algorithm was developed for a Masters-Thesis at Utrecht University.
The corresponding Thesis can be found at: https://studenttheses.uu.nl/handle/20.500.12932/44308

Code needs to be run with an MPI executable like mpiexec with a minimum of 2 processes.

The main to execute file is the Parallel_CostDistance.py.

Commandline arguments:
- -c, --cost: cost raster file
- -s, --source: source raster file
- o, --output: output filename
- p, --partition_size: partition size to be used, default = 250
- t, --time: store the runtime of the execution

Currently, the program can use geotiff files as input and saves the output raster as a geotiff.
Cell sizes has to be the equal for the x and y direction.

When given no arguments, demo files are used for the cost and source raster.
