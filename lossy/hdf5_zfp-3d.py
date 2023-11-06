#######################################################################
# Copyright (c) 2019-present, Blosc Development Team <blosc@blosc.org>
# All rights reserved.
#
# This source code is licensed under a BSD-style license (found in the
# LICENSE file in the root directory of this source tree)
#######################################################################


from time import time
import numpy as np
import blosc2
import h5py
import hdf5plugin


f = h5py.File("/Users/faltet/Downloads/tomo_00001.h5", "r")
ds = f["/exchange/data"]
shape = ds.shape
dtype = ds.dtype
# chunks = ds.chunks
chunks = (1, 1792, 2048)
print(f"shape: {shape}, dtype: {dtype}, chunks: {chunks}")
codec = 'zstd'
codec2 = blosc2.Codec.ZSTD
clevel = 3
filterdict = {"shuf": hdf5plugin.Blosc2.SHUFFLE, "bshuf": hdf5plugin.Blosc2.BITSHUFFLE}
filter = "bshuf"
filters = filterdict[filter]

step = 4  # do steps that are multiple of 4 (ZFP does not support other values)
chunks2 = (step, 1792, 2048)
blocks = (step, 1792 // 9, 2048 // 8)
print(f"out chunks: {chunks2}, out blocks: {blocks}")

fout = h5py.File(f"/Users/faltet/Downloads/tomo_00001-{codec}-{clevel}-{filter}.h5", "w")
dsout = fout.create_dataset("/exchange/data", shape=shape, dtype=dtype, chunks=chunks2,
                            **hdf5plugin.Blosc2(cname=codec, clevel=clevel, filters=filters),
                            )

fout2 = h5py.File(f"/Users/faltet/Downloads/tomo_00001-{codec}-{clevel}-{filter}-direct.h5", "w")
dsout2 = fout2.create_dataset("/exchange/data", shape=shape, dtype="float32", chunks=chunks2,
                              **hdf5plugin.Blosc2())

cparams = {
    #"clevel": 0,
    #"codec": blosc2.Codec.ZFP_ACC, "codec_meta": 256 - 1,  # two’s complement of negative int8
    #"codec": blosc2.Codec.ZFP_ACC, "codec_meta": 1, # bump 5 -> 6
    #"codec": blosc2.Codec.ZFP_PREC, "codec_meta": 0,
    "codec": blosc2.Codec.ZFP_RATE, "codec_meta": 5,  # bump 5 -> 10
    }
    #"filters": [blosc2.Filter.TRUNC_PREC], "filters_meta": [16],}
    #"codec": blosc2.Codec.LZ4,
    #"filters": [blosc2.Filter.BITSHUFFLE]}
t1 = 0
t1direct = 0
for i in range(0, ds.shape[0], step):
    if i > 0:
        break
    a = ds[i : i + step]
    # print(a.shape, a.dtype)
    t0 = time()
    dsout[i : i + step] = a
    t1 += time() - t0
    t0 = time()
    a = a.astype('float32')
    b2chunk = blosc2.asarray(a, chunks=chunks2, blocks=blocks, cparams=cparams)
    b2frame = b2chunk._schunk.to_cframe()
    # print(f"write {(i,) + a.shape}")
    dsout2.id.write_direct_chunk((i,) + a.shape[1:], b2frame)
    t1direct += time() - t0
    #np.testing.assert_array_equal(a_, b2chunk[:])
    #_a = b2chunk[:].astype('uint16'); np.testing.assert_array_equal(a, _a)
f.close()
print(f"Diff in cratio: {dsout.id.get_storage_size() / dsout2.id.get_storage_size():.3f}")
fout.close()
fout2.close()
print(f"Time for writing with h5py: {t1:.3f} s")
print(f"Time for writing with h5py direct: {t1direct:.3f} s")
