#include <stdio.h>

#include <blosc2.h>
#include <blosc2_grok.h>
#include <grok.h>
#include <hdf5.h>

int main(int argc, const char* argv[]) {
  // Parse command-line arguments
  if (argc < 3) {
    fprintf(stderr, "Usage: %s INPUT_HDF5 OUTPUT_HDF5\n", argv[0]);
    exit(1);
  }

  const char* dst_h5file_path = argv[2];

  // Initialize libraries
  blosc2_init();
  blosc2_grok_init(0, true);

  printf("Blosc2 %s\n", blosc2_get_version_string());

  // Create output file
  hid_t dst_h5file_id;
  dst_h5file_id = H5Fcreate(dst_h5file_path, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
  H5Fclose(dst_h5file_id);

  grk_cparameters grok_cparams;
  grk_compress_set_default_params(&grok_cparams);
  grok_cparams.cod_format = GRK_FMT_JP2;

  // Cleanup
  blosc2_grok_destroy();
  blosc2_destroy();

  return 0;
}
