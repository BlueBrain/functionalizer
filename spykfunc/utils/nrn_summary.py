from __future__ import print_function
"""
Program which takes a nrn_summary with only post_neuron touch count and calculates the pre_neuron counterpart
"""

import h5py
import numpy
from future.builtins import range
from future.utils import iteritems
from collections import defaultdict
from bisect import bisect_left
import logging

class NrnCompleter(object):
    # Please use base2 vals
    _GROUP_SIZE = 1024  # mem usage is 4 * GROUP_SIZE^2 on dense matrixes
    _ARRAY_LEN = _GROUP_SIZE ** 2
    _MAX_OUTBUFFER_LEN = 1024**2  # 1M entries ~ 8MB mem

    def __init__(self, input_filename, output_filename, merge=False):
        self.in_file = h5py.File(input_filename, "r")
        self.outfile = h5py.File(output_filename, "w")
        self.max_id = max(int(x[1:]) for x in self.in_file.keys())
        self._n_neurons = len(self.in_file)
        self._outbuffers = defaultdict(list)
        self._outbuffer_entries = 0
        self._array = None

    # ----
    def store_clear_group(self, group_id_start, gid_start):
        # Common gids for group
        all_gids = numpy.arange(self._GROUP_SIZE, dtype="int32") + gid_start

        for idx_start in range(0, self._ARRAY_LEN, self._GROUP_SIZE):
            idx_end = idx_start + self._GROUP_SIZE
            data_view = self._array[idx_start:idx_end]
            filter_mask = data_view > 0
            filtered_counts = data_view[filter_mask]
            if not len(filtered_counts):
                continue
            filtered_gids = all_gids[filter_mask]
            cur_ds_i = idx_start // self._GROUP_SIZE + group_id_start
            merged_data = numpy.stack((filtered_gids, filtered_counts), axis=1)
            ds_name = "a" + str(cur_ds_i)

            # We have outbuffers since HDF5 appends are extremely expensive
            self._outbuffers[ds_name].append(merged_data)
            self._outbuffer_entries += len(merged_data)

        if self._outbuffer_entries > self._MAX_OUTBUFFER_LEN:
            self._flush_outbuffers()

        # Clean for next block
        self._array.fill(0)

    # ----
    def run(self, sparse=False):
        self._errors = False
        if not sparse:
            self._array = numpy.zeros(self._ARRAY_LEN, dtype="int32")

        id_limit = self.max_id + 1
        # For loop just to control the min-max outer gid
        for id_start in range(0, id_limit, self._GROUP_SIZE):
            id_stop = min(id_limit, id_start+self._GROUP_SIZE)
            logging.info("Group %d - %d", id_start, id_stop)
            postgids = []
            sub_offset = []

            # Init structures for the current group block
            for post_gid in range(id_start, id_stop):
                if ("a" + str(post_gid)) not in self.in_file:
                    continue
                postgids.append(post_gid)
                sub_offset.append(0)

            # For loop to control the inner gid
            for id_start_2 in range(0, id_limit, self._GROUP_SIZE):
                id_stop_2 = min(id_limit, id_start_2 + self._GROUP_SIZE)
                last_section = id_stop_2 == id_limit  # The max inner GID isn't necessarily the max out
                group_max_len = id_stop_2 - id_start_2
                for i, post_gid in enumerate(postgids):
                    ds_name = "a"+str(post_gid)
                    ds = self.in_file[ds_name]
                    cur_offset = sub_offset[i]
                    data = ds[cur_offset:cur_offset+group_max_len]
                    for row in data:
                        pre_gid, touch_count = row
                        if not last_section and pre_gid >= id_stop_2:
                            # Stop and save iteration state here, except in last section
                            sub_offset[i] = cur_offset
                            break
                        cur_offset += 1
                        if not sparse:
                            self._array[(pre_gid - id_start_2) * self._GROUP_SIZE + post_gid-id_start] = touch_count
                        else:
                            row[0] = post_gid
                            self._outbuffers["a"+str(pre_gid)].append(row.reshape((1,2)))
                            self._outbuffer_entries += 1
                    sub_offset[i] = cur_offset

                if not sparse:
                    self.store_clear_group(id_start_2, id_start)
                else:
                    if self._outbuffer_entries > self._MAX_OUTBUFFER_LEN:
                        self._flush_outbuffers()

        logging.debug("Final buffer flush")
        self._flush_outbuffers(final=True)

    # ----
    def _flush_outbuffers(self, final=False):
        """
        Flush output buffers to destination file
        :param final: If it's the last flush, so that datasets dion't have to be resizable
        :return:
        """
        for ds_name, ds_parts in iteritems(self._outbuffers):
            merged_data = numpy.concatenate(ds_parts)
            if ds_name not in self.outfile:
                if final:
                    self.outfile.create_dataset(ds_name, data=merged_data)
                else:
                    self.outfile.create_dataset(ds_name, data=merged_data, chunks=(100, 2), maxshape=(None,2))
            else:
                ds = self.outfile[ds_name]
                cur_length = len(ds)
                ds.resize(cur_length + len(merged_data), axis=0)
                ds[cur_length:] = merged_data

        self._outbuffers = defaultdict(list)
        self._outbuffer_entries = 0


    # *********************************
    # Validation
    # *********************************
    def validate(self, reverse=False):
        """
        Validates, by checking 1-1 if the files were correctly reversed.
        NOTE the performance is expected to be bad, since we shall not introduce complex optimizations
        :param reverse: Checking if all entries in the generated file are there in the original
        :return:
        """
        in_file = self.in_file if not reverse else self.outfile
        out_file = self.outfile if not reverse else self.in_file

        problematic_grps = set()
        missing_points = []
        for name, group in in_file.iteritems():
            for id1, cnt in group:
                if cnt == 0:
                    continue
                try:
                    ds = out_file["a" + str(id1)]
                except Exception:
                    problematic_grps.add(id1)
                    continue

                id2 = int(name[1:])
                posic = bisect_left(ds[:, 0], id2)  # logN search

                if posic == len(ds) or ds[posic, 0] != id2:
                    missing_points.append((id1, id2))
                elif ds[posic, 1] != cnt:
                    # This is really not supposed to happen
                    logging.error("Different values in ID {}-{}. Counts: {}. Entry: {}".format(name, id1, cnt, ds[posic]))
                    self._errors = True

        if problematic_grps:
            logging.error("Problematic grps: %s", str(list(problematic_grps)))
            self._errors = True
        if missing_points:
            logging.error("Missing points: %s", str(missing_points))
            self._errors = True

    # ----
    def check_ordered(self):
        for name, group in self.in_file.iteritems():
            if not numpy.array_equal(numpy.sort(group[:,0]), group[:,0]):
                logging.error("Dataset %s not ordered!", name)
                self._errors = True


# **************************
# Tests
# **************************

if __name__ == "__main__":
    cter = NrnCompleter("spykfunc_output/nrn_summary0.h5", "spykfunc_output/nrn_summary.h5")
    cter.run(sparse=True)

    cter.check_ordered()
    assert not cter._errors, "Order errors were found"

    print("Validating...")
    cter.validate()

    print("Reverse validating...")
    cter.validate(reverse=True)
