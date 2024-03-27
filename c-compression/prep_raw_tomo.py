#!/usr/bin/env python
# Export Blosc2 n-dimensional array in `SRC_TOMOGRAPHY_PATH`
# to uncompressed dataset ``DST_TOMOGRAPHY_PATH:/tomo``
# with reduced resolution and one image per chunk.

import math

import blosc2
import h5py


SRC_TOMOGRAPHY_PATH = '../data/lung_raw_slice.b2nd'
DST_TOMOGRAPHY_PATH = 'tomo-raw.h5'
DST_TOMOGRAPHY_NAME = 'tomo'
SHRINK = 2  # half the resolution


src_tomo = blosc2.open(SRC_TOMOGRAPHY_PATH)
assert src_tomo.ndim == 3

# Reduce image resolution.
dst_tomo_shape = (src_tomo.shape[0],
                  *(math.ceil(d / SHRINK) for d in src_tomo.shape[1:]))
# One image per chunk.
dst_tomo_chunks = (1, *dst_tomo_shape[1:])

with h5py.File(DST_TOMOGRAPHY_PATH, 'w') as h5f:
    dst_tomo = h5f.create_dataset(DST_TOMOGRAPHY_NAME,
                                  dtype=src_tomo.dtype,
                                  shape=dst_tomo_shape,
                                  chunks=dst_tomo_chunks)
    for i in range(src_tomo.shape[0]):
        dst_tomo[i] = src_tomo[i][::SHRINK, ::SHRINK].reshape(dst_tomo_chunks)
