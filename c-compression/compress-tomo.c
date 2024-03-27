#include <stdio.h>

#include <blosc2.h>

int main(void) {
  printf("Blosc2 %s\n", blosc2_get_version_string());
  return 0;
}
