#include <stdio.h>

#include <blosc2.h>
#include <blosc2_grok.h>
#include <grok.h>

int main(void) {
  blosc2_init();
  blosc2_grok_init(0, true);

  printf("Blosc2 %s\n", blosc2_get_version_string());

  grk_cparameters grok_cparams;
  grk_compress_set_default_params(&grok_cparams);
  grok_cparams.cod_format = GRK_FMT_JP2;

  blosc2_grok_destroy();
  blosc2_destroy();

  return 0;
}
