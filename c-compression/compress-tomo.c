#include <stdio.h>

#include <blosc2.h>
#include <blosc2_grok.h>
#include <grok.h>
#include <hdf5.h>

#define SRC_TOMOGRAPHY_NAME "/tomo"

#define FAIL(_LABEL) { status = EXIT_FAILURE; goto _LABEL; }
#define CHKPOS(_EXPR, _LABEL) { if ((_EXPR) < 0) FAIL(_LABEL) }

int main(int argc, const char* argv[]) {
  int status = EXIT_SUCCESS;

  // Parse command-line arguments
  if (argc < 3) {
    fprintf(stderr, "Usage: %s INPUT_HDF5 OUTPUT_HDF5\n", argv[0]);
    FAIL(err_parse_args);
  }

  const char* src_h5file_path = argv[1];
  const char* dst_h5file_path = argv[2];

  // Initialize libraries
  blosc2_init();
  blosc2_grok_init(0, true);

  printf("Blosc2 %s\n", blosc2_get_version_string());

  // Open input dataset
  hid_t src_h5file_id;
  CHKPOS(src_h5file_id = H5Fopen(src_h5file_path, H5F_ACC_RDONLY, H5P_DEFAULT),
         err_open_srcf);

  hid_t src_h5dset_id;
  CHKPOS(src_h5dset_id = H5Dopen2(src_h5file_id, SRC_TOMOGRAPHY_NAME, H5P_DEFAULT),
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

  // Create output file
  hid_t dst_h5file_id;
  CHKPOS(dst_h5file_id = H5Fcreate(dst_h5file_path, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT),
         err_open_dstf);

  grk_cparameters grok_cparams;
  grk_compress_set_default_params(&grok_cparams);
  grok_cparams.cod_format = GRK_FMT_JP2;

  // Cleanup
  H5Fclose(dst_h5file_id);

  err_open_dstf:
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
