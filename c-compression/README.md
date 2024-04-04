# Blosc2 Grok C compression example

The `compress-tomo.c` source shows how to use Blosc2's Grok (JPEG 2000) compression codec plugin to process a tomography dataset. The dataset is a stack of greyscale images in an HDF5 input file (`tomo-raw.h5`), stored as a 3D array of integer values. The program creates an output HDF5 file (`tomo.h5`) with a dataset of the same name, shape and type, where each image is encoded using JPEG 2000 in its own HDF5 chunk. Each such chunk is stored as a Blosc2 array (super-chunk) with a single chunk consisting of a single block, so that Blosc2 threading is not used, but Grok is able to use threading itself on the whole image.

The input file is prepared by the script `prep_raw_tomo.py`, which uses `h5py` and `python-blosc2` to convert a Blosc2 dataset in `../data/` with the original tomography. The resulting `tomo-raw.h5` contains the tomography dataset with no compression whatsoever, so that you may compare sizes with the final `tomo.h5`.

## Usage

To build and run the test you need the following software already installed in your system:

- Make
- coreutils
- Python 3
- C compiler toolchain & libraries
- HDF5 headers & libraries

The accompanying `Makefile` is used to setup a Python virtual environment in the current directory for the preparation script and Blosc2 plugin library detection code to work (so you may need an Internet connection).  You should be able to execute all steps by running `make`.

If you need to setup other system-specific compile or link options (like non-standard include or library paths), you may use the `CFLAGS` and `LDFLAGS` variables, either from the environment or in `Makefile`.

When the compression program runs, it will report some statistics.  If you want to tune compression parameters (like target ratio or number of threads), you may look for and edit lines marked with `tune` in `compress-tomo.c` and run `make` again.

To remove generated files, run `make clean`.
