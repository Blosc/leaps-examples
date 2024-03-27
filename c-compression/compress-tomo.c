#include <stdio.h>

#include <blosc2.h>
#include <grok.h>

int main(void) {
  printf("Blosc2 %s\n", blosc2_get_version_string());

  grk_cparameters grok_cparams;
  grk_compress_set_default_params(&grok_cparams);
  grok_cparams.cod_format = GRK_FMT_JP2;

  return 0;
}
