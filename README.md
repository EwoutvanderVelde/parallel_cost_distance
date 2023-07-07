# Parallel cost distance algorithm

A commandline parallel algorithm to do cost cost distance analysis.

This algorithm was developed for a Masters-Thesis at Utrecht University.
The corresponding Thesis can be found at: TODO: Add link when published.

Code needs to be run with an MPI executable like mpiexec.

The main to execute file is the Parallel_CostDistance.py.

Commandline arguments:
- -c, --cost: cost raster file
- -s, --source: source raster file
- o, --output: output filename
- p, --partition_size: partition size to be used
- t, --time: store the runtime of the execution

Currently, the program can use geotiff files as input and saves the output raster as a geotiff.
Cell sizes has to be the equal for the x and y direction.

