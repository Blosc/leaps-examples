PY_VENV := VENV
PY := $(PY_VENV)/bin/python


.PHONY:	all clean

all:	$(PY_VENV) tomo-raw.h5

$(PY_VENV):
	mkdir $(PY_VENV)
	python3 -m venv $(PY_VENV)
	$(PY) -m pip install blosc2 blosc2-grok h5py

tomo-raw.h5:	prep_raw_tomo.py ../data/lung_raw_slice.b2nd
	$(PY) $^ $@

clean:
	rm -rf $(PY_VENV) tomo-raw.h5
