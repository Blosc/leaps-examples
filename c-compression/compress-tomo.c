#include <stdio.h>
#include <time.h>

#include <blosc2.h>
#include <blosc2/codecs-registry.h>
#include <blosc2_grok.h>
#include <grok.h>
#include <hdf5.h>
#include <hdf5_hl.h>  // only for Caterva2 attribute

#define TOMOGRAPHY_NAME "/tomo"

/* Filter ID registered with the HDF Group, */
/* see <https://portal.hdfgroup.org/documentation/hdf5-docs/registered_filter_plugins.html>. */
#define FILTER_BLOSC2 32026

#define FAIL(_LABEL) { status = EXIT_FAILURE; goto _LABEL; }
#define CHKPOS(_EXPR, _LABEL) { if ((_EXPR) < 0) FAIL(_LABEL) }

int main(int argc, const char* argv[]) {
  int status = EXIT_SUCCESS;

  //// Parse command-line arguments

  if (argc < 3) {
    fprintf(stderr, "Usage: %s INPUT_HDF5 OUTPUT_HDF5\n", argv[0]);
    FAIL(err_parse_args);
  }

  const char* src_h5file_path = argv[1];
  const char* dst_h5file_path = argv[2];

  //// Initialize libraries

  blosc2_init();
  blosc2_grok_init(0, true);
  printf("Blosc2 %s\n", blosc2_get_version_string());

  //// Open input dataset

  hid_t src_h5file_id;
  CHKPOS(src_h5file_id = H5Fopen(src_h5file_path, H5F_ACC_RDONLY, H5P_DEFAULT),
         err_open_srcf);

  hid_t src_h5dset_id;
  CHKPOS(src_h5dset_id = H5Dopen2(src_h5file_id, TOMOGRAPHY_NAME, H5P_DEFAULT),
         err_open_srcds);

  hid_t src_h5dssp_id;
  CHKPOS(src_h5dssp_id = H5Dget_space(src_h5dset_id),
         err_open_srcdssp);

  int dset_rank;
  hsize_t dset_shape[H5S_MAX_RANK];
  CHKPOS(dset_rank = H5Sget_simple_extent_dims(src_h5dssp_id, dset_shape, NULL),
         err_check_srcdssp);
  if (dset_rank != 3) {
    fprintf(stderr, "source tomography must have 3 dimensions, not %d\n", dset_rank);
    FAIL(err_check_srcdssp);
  }

  hid_t dset_h5type_id;
  CHKPOS(dset_h5type_id = H5Dget_type(src_h5dset_id),
         err_open_srcdsty);

  //// Create output dataset

  hid_t dst_h5plst_id;
  CHKPOS(dst_h5plst_id = H5Pcreate(H5P_DATASET_CREATE),
         err_make_dstpl);

  hsize_t dst_chunk_shape[] = {1, dset_shape[1], dset_shape[2]};  // one 2D image per chunk
  CHKPOS(H5Pset_chunk(dst_h5plst_id, 3, dst_chunk_shape),
         err_conf_dstpl);

  size_t dset_type_size = H5Tget_size(dset_h5type_id);
  size_t chunk_size = (dst_chunk_shape[0] * dst_chunk_shape[1] * dst_chunk_shape[2]
                       * dset_type_size);

  // These compression parameters for filter are mostly descriptive,
  // as the compression will be done manually.
  int cd_values[8 + 3] = {
    1 /* filter rev */,
    chunk_size /* block size (1 per chunk) */, dset_type_size, chunk_size,
    5 /* comp level (ign) */, 0 /* shuffle (ign) */, BLOSC_CODEC_GROK,
    dset_rank,
    dst_chunk_shape[0], dst_chunk_shape[1], dst_chunk_shape[2],
  };
  CHKPOS(H5Pset_filter(dst_h5plst_id, FILTER_BLOSC2, H5Z_FLAG_OPTIONAL, 8 + 3, cd_values),
         err_conf_dstpl);

  hid_t dst_h5dssp_id;
  CHKPOS(dst_h5dssp_id = H5Screate_simple(dset_rank, dset_shape, NULL),
         err_make_dstsp);

  hid_t dst_h5file_id;
  CHKPOS(dst_h5file_id = H5Fcreate(dst_h5file_path, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT),
         err_make_dstf);

  hid_t dst_h5dset_id;
  CHKPOS(dst_h5dset_id = H5Dcreate2(dst_h5file_id, TOMOGRAPHY_NAME, dset_h5type_id, dst_h5dssp_id,
                                    H5P_DEFAULT, dst_h5plst_id, H5P_DEFAULT),
         err_make_dstds);

  // Only needed to get a nice display in Caterva2 web viewer
  CHKPOS(H5LTset_attribute_string(dst_h5file_id, TOMOGRAPHY_NAME,
                                  "contenttype", "tomography"),
         err_c2attr_dstds);

  //// Prepare compression parameters for individual chunks

  // **Note:** The Pillow-like parameters allowed by `blosc2_grok.set_params_defaults` in Python
  // are not available here, only the ones supported by Grok itself.  See the Grok header at
  // <https://github.com/GrokImageCompression/grok/blob/a84ac2592e581405a976a00cf9e6f03cab7e2481/src/lib/core/grok.h#L975>
  // for more information.

  blosc2_grok_params b2gk_params = {0};
  grk_compress_set_default_params(&(b2gk_params.compressParams));
  b2gk_params.compressParams.cod_format = GRK_FMT_JP2;
  b2gk_params.compressParams.numlayers = 1;
  b2gk_params.compressParams.allocationByRateDistoration = true;
  b2gk_params.compressParams.layer_rate[0] = 10.0;  // tune
  b2gk_params.compressParams.numThreads = 1;  // tune
  grk_set_default_stream_params(&(b2gk_params.streamParams));

  blosc2_cparams b2_cparams = BLOSC2_CPARAMS_DEFAULTS;
  b2_cparams.compcode = BLOSC_CODEC_GROK;
  b2_cparams.typesize = dset_type_size;
  for (int i = 0; i < BLOSC2_MAX_FILTERS; i++)
    b2_cparams.filters[i] = 0;
  b2_cparams.splitmode = BLOSC_NEVER_SPLIT;
  b2_cparams.codec_params = &b2gk_params;
  blosc2_dparams b2_dparams = BLOSC2_DPARAMS_DEFAULTS;

  blosc2_storage b2_storage = {.cparams = &b2_cparams, .dparams = &b2_dparams};

  b2nd_context_t *chunk_b2ctx;
  {
    // We will be maping each HDF5 chunks maps to a B2ND array of 1 chunk with 1 block.
    int32_t chunk_b2shape[] = {dst_chunk_shape[0], dst_chunk_shape[1],dst_chunk_shape[2]};
    chunk_b2ctx = b2nd_create_ctx(&b2_storage, dset_rank, dst_chunk_shape /* shape */,
                                  chunk_b2shape /* chunk */, chunk_b2shape /* block */,
                                  NULL, 0, NULL, 0);
  }
  if (!chunk_b2ctx) {
    fprintf(stderr, "failed to create chunk B2ND context\n");
    FAIL(err_make_b2ctx);
  }

  //// Read and write individual images

  uint8_t *chunk_data;
  chunk_data = malloc(chunk_size);
  if (!chunk_data) {
    fprintf(stderr, "failed to allocate chunk memory\n");
    FAIL(err_alloc_chunk);
  }

  hid_t mem_h5cksp_id;
  CHKPOS(mem_h5cksp_id = H5Screate_simple(dset_rank, dst_chunk_shape, NULL),
         err_make_memsp);
  b2nd_array_t *chunk_b2arr;  // to be overwritten with each image
  if (b2nd_uninit(chunk_b2ctx, &chunk_b2arr) < 0) {
    fprintf(stderr, "failed to create B2ND array\n");
    FAIL(err_make_b2arr);
  }

  struct timespec start_ts;
  clock_gettime(CLOCK_MONOTONIC, &start_ts);

  hsize_t chunk_offset[] = {0, 0, 0};
  const int64_t chunk_start[] = {0, 0, 0};
  for (int i = 0; i < dset_shape[0]; i++) {
    chunk_offset[0] = i;
    if (H5Sselect_hyperslab(src_h5dssp_id, H5S_SELECT_SET,
                            chunk_offset, NULL, dst_chunk_shape, NULL) < 0) {
      fprintf(stderr, "failed to select image #%d\n", i);
      FAIL(err_read_image);
    }
    if (H5Dread(src_h5dset_id, dset_h5type_id, mem_h5cksp_id, src_h5dssp_id,
                H5P_DEFAULT, chunk_data) < 0) {
      fprintf(stderr, "failed to read image #%d\n", i);
      FAIL(err_read_image);
    }

    if (b2nd_set_slice_cbuffer(chunk_data, dst_chunk_shape, chunk_size,
                               chunk_start, dst_chunk_shape,
                               chunk_b2arr) < 0) {
      fprintf(stderr, "failed to compress image #%d\n", i);
      FAIL(err_read_image);
    }
    uint8_t *cframe;
    int64_t cframe_size;
    bool free_cframe;
    if (b2nd_to_cframe(chunk_b2arr, &cframe, &cframe_size, &free_cframe) < 0) {
      fprintf(stderr, "failed to serialize compressed image #%d\n", i);
      FAIL(err_read_image);
    }

    herr_t status = H5Dwrite_chunk(dst_h5dset_id, H5P_DEFAULT, 0,
                                   chunk_offset, (size_t)(cframe_size), cframe);
    if (free_cframe)
      free(cframe);
    if (status < 0) {
      fprintf(stderr, "failed to write compressed chunk for image #%d\n", i);
      FAIL(err_read_image);
    }
  }

  struct timespec stop_ts;
  clock_gettime(CLOCK_MONOTONIC, &stop_ts);

  double elapsed = ((stop_ts.tv_sec - start_ts.tv_sec)
                    + (stop_ts.tv_nsec - start_ts.tv_nsec) / 1e9);
  printf("Compressed and stored %ld images of %ld bytes each in %f seconds total"
         " (%f seconds/image).\n",
         dset_shape[0], chunk_size, elapsed, elapsed / dset_shape[0]);

  //// Cleanup

  err_read_image:
  b2nd_free(chunk_b2arr);
  err_make_b2arr:
  H5Sclose(mem_h5cksp_id);
  err_make_memsp:
  free(chunk_data);
  err_alloc_chunk:

  b2nd_free_ctx(chunk_b2ctx);
  err_make_b2ctx:

  err_c2attr_dstds:
  H5Dclose(dst_h5dset_id);
  err_make_dstds:
  H5Fclose(dst_h5file_id);
  err_make_dstf:
  H5Sclose(dst_h5dssp_id);
  err_make_dstsp:
  err_conf_dstpl:
  H5Pclose(dst_h5plst_id);
  err_make_dstpl:

  H5Tclose(dset_h5type_id);
  err_open_srcdsty:
  err_check_srcdssp:
  H5Sclose(src_h5dssp_id);
  err_open_srcdssp:
  H5Dclose(src_h5dset_id);
  err_open_srcds:
  H5Fclose(src_h5file_id);
  err_open_srcf:

  blosc2_grok_destroy();
  blosc2_destroy();

  err_parse_args:
  return status;
}
