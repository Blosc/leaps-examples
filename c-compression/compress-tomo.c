#include <stdio.h>

#include <blosc2.h>
#include <blosc2_grok.h>
#include <grok.h>
#include <hdf5.h>

#define TOMOGRAPHY_NAME "/tomo"

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

  // Create output dataset
  hid_t dst_h5plst_id;
  CHKPOS(dst_h5plst_id = H5Pcreate(H5P_DATASET_CREATE),
         err_make_dstpl);
  hsize_t dst_chunk_shape[] = {1, dset_shape[1], dset_shape[2]};  // one 2D image per chunk
  CHKPOS(H5Pset_chunk(dst_h5plst_id, 3, dst_chunk_shape),
         err_conf_dstpl);

  grk_cparameters grok_cparams;
  grk_compress_set_default_params(&grok_cparams);
  grok_cparams.cod_format = GRK_FMT_JP2;

  // TODO: configure compression in property list

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

  // Cleanup
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
