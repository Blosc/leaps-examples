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
from skimage.metrics import structural_similarity as ssim


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

step = 1
chunks2 = (step, 1792, 2048)
blocks = (step, 1792 // 9, 2048 // 8)

fout = h5py.File(f"/Users/faltet/Downloads/tomo_00001-{codec}-{clevel}-{filter}.h5", "w")
dsout = fout.create_dataset("/exchange/data", shape=shape, dtype=dtype, chunks=chunks2,
                            **hdf5plugin.Blosc2(cname=codec, clevel=clevel, filters=filters))

fout2 = h5py.File(f"/Users/faltet/Downloads/tomo_00001-{codec}-{clevel}-{filter}-direct.h5", "w")
dsout2 = fout2.create_dataset("/exchange/data", shape=shape, dtype=dtype, chunks=chunks2,
                              **hdf5plugin.Blosc2())

cparams = {"clevel": clevel, "codec": codec2, "filters": [filters]}
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
    b2chunk = blosc2.asarray(a, chunks=chunks2, blocks=blocks, cparams=cparams)
    b2frame = b2chunk._schunk.to_cframe()
    print(f"write {a.shape, b2chunk.shape}")
    dsout2.id.write_direct_chunk((i,) + a.shape[1:], b2frame)
    t1direct += time() - t0
    _a = np.squeeze(a[:])
    a2 = np.squeeze(b2chunk[:])
    print(f"{_a.shape, a2.shape}")
    lossy_a = a2 #.astype('float64')
    mssim = ssim(_a, lossy_a,  data_range=lossy_a.max() - lossy_a.min())
f.close()
print(f"SSIM: {mssim:.3f}")
print(f"Diff in cratio: {dsout.id.get_storage_size() / dsout2.id.get_storage_size():.3f}")
fout.close()
fout2.close()
print(f"Time for writing with h5py: {t1:.3f} s")
print(f"Time for writing with h5py direct: {t1direct:.3f} s")
