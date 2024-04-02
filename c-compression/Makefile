PY_VENV := VENV
PY := $(PY_VENV)/bin/python

OUT := $(PY_VENV) tomo-raw.h5 compress-tomo tomo.h5


.PHONY:	all clean

all:	$(OUT)

# The Python virtual environment is used for
# (i) installing the dependencies of the raw tomo prep script, and
# (ii) installing the Blosc2 Grok plugin to access its C headers & libraries.
$(PY_VENV):
	mkdir $(PY_VENV)
	python3 -m venv $(PY_VENV)
	$(PY) -m pip install blosc2 blosc2-grok h5py

tomo-raw.h5:	prep_raw_tomo.py ../data/lung_raw_slice.b2nd
	$(PY) $^ $@

compress-tomo tomo.h5:	PY_SITE=$(dir $(shell $(PY) -c 'import blosc2; print(blosc2.__path__[0])'))

compress-tomo:	compress-tomo.c
	$(CC) -o $@ $< \
	      -I$(PY_SITE)/include -L$(PY_SITE)/blosc2_grok -lblosc2_grok \
	      -I$(PY_VENV)/include -L$(PY_VENV)/lib -lblosc2 \
	      -I$(wildcard $(PY_SITE)/include/grok-*) -L$(PY_SITE)/lib64 -lgrokj2k -lstdc++ \
	      -lhdf5

# The environment's Python needs to be found first since
# Blosc2 executes it to find the location of the plugin library.
tomo.h5:	compress-tomo tomo-raw.h5
	env LD_LIBRARY_PATH="$(PY_VENV)/lib:$(PY_SITE)/blosc2_grok:$(LD_LIBRARY_PATH)" \
	    PATH="$(PY_VENV)/bin:$(PATH)" \
	    ./$^ $@

clean:
	rm -rf $(OUT)
