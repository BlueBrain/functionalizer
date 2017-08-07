# -------------------------------
# Makefile for quick building mod
# -------------------------------
override CFLAGS += -Wall -Wextra -fPIC -g -O2 -Wno-strict-aliasing
LDFLAGS = -shared -lhdf5 #-lmvdtool/mvdtool.so
PYTHONHOME ?= /usr
MAIN_DIR="../.."
REQ_INCLUDES = "$(PYTHONHOME)/include/python2.7" "$(MAIN_DIR)/functionalizer" "$(MAIN_DIR)/deps/hadoken/include" "$(MAIN_DIR)/deps/mvd-tool/include" "$(MAIN_DIR)/deps/mvd-tool/deps/highfive/include" "/usr/include/libxml2"

SRCS = $(wildcard *.pyx)
CPPS = $(patsubst %.pyx,%.cpp,$(SRCS))
MODS = $(patsubst %.pyx,%.so,$(SRCS))

all: $(MODS)
clean:
	rm -f $(CPPS) $(MODS)

#cython to cpp
%.cpp: %.pyx
	cython --cplus --fast-fail -I mvdtool $<

#cpp to module
%.so: %.cpp
	$(CXX) $(LDFLAGS) $(CFLAGS) $(foreach var, $(REQ_INCLUDES), -I$(var)) -o $@ $<
