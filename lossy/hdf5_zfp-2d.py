#######################################################################
# Copyright (c) 2019-present, Blosc Development Team <blosc@blosc.org>
# All rights reserved.
#
# This source code is licensed under a BSD-style license (found in the
# LICENSE file in the root directory of this source tree)
#######################################################################


import numpy as np
from skimage.metrics import structural_similarity as ssim
from time import time
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

chunks2 = (1, 1792, 2048)

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
    #"codec": blosc2.Codec.ZFP_ACC, "codec_meta": 6, # no competitive
    #"codec": blosc2.Codec.ZFP_PREC, "codec_meta": 0, # no competitive
    "codec": blosc2.Codec.ZFP_RATE, "codec_meta": 20,  # bump 20 -> 25
    }
    #"filters": [blosc2.Filter.TRUNC_PREC], "filters_meta": [16],}
    #"codec": blosc2.Codec.LZ4,
    #"filters": [blosc2.Filter.BITSHUFFLE]}
blocks = (1792 // 9, 2048 // 8)
#blocks = (1792, 2048)
t1 = 0
t1direct = 0
for i in range(ds.shape[0]):
    if i > 0:
        break
    a = ds[i]
    t0 = time()
    dsout[i] = a
    t1 += time() - t0
    t0 = time()
    a_ = a.astype('float32')
    print(a.shape, a.dtype)
    b2chunk = blosc2.asarray(a_, chunks=a.shape, blocks=blocks, cparams=cparams)
    b2frame = b2chunk._schunk.to_cframe()
    # print(f"write {(i,) + a.shape}")
    dsout2.id.write_direct_chunk((i,) + a.shape, b2frame)
    t1direct += time() - t0
    _a = a.astype('int16')
    lossy_a = b2chunk[:].astype('int16')
    print(f"{_a.shape, lossy_a.shape}")
    ssim_ = ssim(_a, lossy_a,  data_range=lossy_a.max() - lossy_a.min())
    #np.testing.assert_array_equal(a_, lossy_a)
    #_a = b2chunk[:].astype('uint16'); np.testing.assert_array_equal(a, _a)
f.close()
print(f"SSIM: {ssim_:.3f}")
print(f"Diff in cratio: {dsout.id.get_storage_size() / dsout2.id.get_storage_size():.3f}")
fout.close()
fout2.close()
print(f"Time for writing with h5py (lossless, bitshuffle + zstd-level3): {t1:.3f} s")
print(f"Time for writing with h5py direct (ZFP): {t1direct:.3f} s")
