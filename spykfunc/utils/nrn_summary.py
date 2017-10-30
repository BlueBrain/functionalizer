from __future__ import print_function
"""
Program which takes a nrn_summary with only post_neuron touch count and calculates the pre_neuron counterpart
"""

import h5py
import numpy
from future.builtins import range

class NrnCompleter(object):
    # mem usage is 4 * GROUP_SIZE^2 (yes, squared!)
    # Please use base2 vals
    _GROUP_SIZE = 1024
    _ARRAY_LEN = _GROUP_SIZE ** 2

    def __init__(self, input_filename, output_filename):
        self.in_file = h5py.File(input_filename)
        self.outfile = h5py.File(output_filename, "w")
        array_len = self._GROUP_SIZE ** 2
        self.max_id = max(int(x[1:]) for x in self.in_file.keys())
        self._n_neurons = len(self.in_file)
        self._array = numpy.zeros(array_len, dtype="i4")

    def store_clear_group(self, group_id_start, gid_start):
        # For each neuron in the group
        # Move the values from the array into the Hdf5

        # Common gids for group
        all_gids = numpy.arange(self._GROUP_SIZE) + gid_start

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
            if ds_name not in self.outfile:
                self.outfile.create_dataset(ds_name, data=merged_data, chunks=(20, 2), maxshape=(None,2))
            else:
                ds = self.outfile[ds_name]
                cur_length = len(ds)
                ds.resize(cur_length + len(filtered_counts), axis=0)
                ds[cur_length:] = merged_data

        # Clean for next block
        self._array.fill(0)

    # ---
    def run(self):

        # For loop just to control the min-max outer gid
        for id_start in range(0, self.max_id, self._GROUP_SIZE):
            id_stop = min(self.max_id, id_start+self._GROUP_SIZE)

            print("Group {} - {}".format(id_start, id_stop))

            postgids = []
            sub_offset = []

            # Init structures for the current group block
            for post_gid in range(id_start, id_stop):
                if ("a" + str(post_gid)) not in self.in_file:
                    continue
                postgids.append(post_gid)
                sub_offset.append(0)

            # For loop to control the inner gid
            for id_start_2 in range(0, self.max_id, self._GROUP_SIZE):
                id_stop_2 = min(self.max_id, id_start_2 + self._GROUP_SIZE)
                group_max_len = id_stop_2 - id_start_2
                for i, post_gid in enumerate(postgids):
                    cur_offset = sub_offset[i]
                    data = self.in_file["a"+str(post_gid)][cur_offset:cur_offset+group_max_len]
                    for pre_gid, touch_count in data:
                        if pre_gid >= id_stop_2:
                            sub_offset[i] = cur_offset
                            break
                        cur_offset += 1
                        self._array[(pre_gid - id_start_2) * self._GROUP_SIZE + post_gid-id_start] = touch_count
                    sub_offset[i] = cur_offset

                self.store_clear_group(id_start_2, id_start)



if __name__ == "__main__":
    cter = NrnCompleter("spykfunc_output/nrn_summary0.h5", "spykfunc_output/nrn_summary.h5")
    cter.run()
