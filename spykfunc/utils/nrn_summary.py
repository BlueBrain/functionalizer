"""
Program which takes a nrn_summary with only post_neuron touch count and calculates the pre_neuron counterpart
"""
from __future__ import print_function
import h5py
import numpy
from docopt import docopt
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
    _OPTS_DEFAULT = dict(verbose=0)

    def __init__(self, input_filename, **opts):
        self._in_filename = input_filename
        self.in_file = h5py.File(input_filename, "r")
        self.outfile = None
        self.max_id = max(int(x[1:]) for x in self.in_file.keys())
        self._n_neurons = len(self.in_file)
        self._outbuffers = defaultdict(list)
        self._outbuffer_entries = 0
        self._array = None
        self._opts = self._OPTS_DEFAULT.copy()
        self._opts.update(opts)
        if "logger" in opts:
            self.logger = opts["logger"]
        else:
            self.logger = logging
            if self._opts["verbose"] == 1:
                logging.basicConfig(level=logging.INFO)
            elif self._opts["verbose"] == 2:
                logging.basicConfig(level=logging.DEBUG)

    # ----
    def create_transposed(self, output_filename=None, sparse=False):
        """
        Create a transposed version of the h5 file, datasets ("columns") become rows, rows become datasets
        :param sparse: If the h5 file matrix is not dense (case of touches) we can use sparse to avoid creating an \
        intermediate matrix structure and use a simple algorithm.
        """
        output_filename = output_filename or self._in_filename + ".T"
        self.outfile = h5py.File(output_filename, "w")
        id_limit = self.max_id + 1
        print("[TRANSPOSING] %d neurons in blocks of %dx%d (mode: %s)" %
              (id_limit, self._GROUP_SIZE, self._GROUP_SIZE, "sparse" if sparse else "dense_matrix"))

        if not sparse:
            # With dense datasets we use a temporary array
            self._array = numpy.zeros(self._ARRAY_LEN, dtype="int32")

        # For loop just to control the min-max outer gid
        for id_start in range(0, id_limit, self._GROUP_SIZE):
            id_stop = min(id_limit, id_start+self._GROUP_SIZE)
            postgids = []
            sub_offset = []

            self.logger.info("Group %d-%d [%3d%%]",
                             id_start, id_stop, (id_stop-id_start)*100//id_limit)

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
                            self._outbuffers["a"+str(pre_gid)].append(row.reshape((1, 2)))
                            self._outbuffer_entries += 1
                    sub_offset[i] = cur_offset

                if not sparse:
                    self._store_clear_group(id_start_2, id_start)
                else:
                    if self._outbuffer_entries > self._MAX_OUTBUFFER_LEN:
                        self._flush_outbuffers()

        self.logger.debug("Final buffer flush")
        self._flush_outbuffers(final=True)
        self.logger.info("Transposing complete")

    # ----
    def _store_clear_group(self, group_id_start, gid_start):
        """
        Write the intermediate matrix, clearing it to the next iteration
        """
        # Common gids for group
        all_gids = numpy.arange(self._GROUP_SIZE, dtype="int32") + gid_start
        self.logger.debug("Processing matrix to buffers [group offset: %d, base gid: %d]",
                          group_id_start, gid_start)

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
            self.logger.debug("Flushing buffers")
            self._flush_outbuffers()

        # Clean for next block
        self._array.fill(0)

    # ----
    def _flush_outbuffers(self, final=False):
        """
        Flush output buffers to destination file
        :param final: If True, non-existing datasets are created non-resizable, optimizing space
        """
        for ds_name, ds_parts in iteritems(self._outbuffers):
            merged_data = numpy.concatenate(ds_parts)
            if ds_name not in self.outfile:
                if final:
                    self.outfile.create_dataset(ds_name, data=merged_data)
                else:
                    self.outfile.create_dataset(ds_name, data=merged_data, chunks=(100, 2), maxshape=(None, 2))
            else:
                ds = self.outfile[ds_name]
                cur_length = len(ds)
                ds.resize(cur_length + len(merged_data), axis=0)
                ds[cur_length:] = merged_data

        self._outbuffers = defaultdict(list)
        self._outbuffer_entries = 0

    # ----
    def merge(self, merged_filename=None):
        """
        Merger of both forward and reverse matrixes (afferent and efferent touch count)
        :param merged_filename: The name of the output merged file
        """
        merged_filename = merged_filename or self._in_filename + ".merged"
        merged_file = h5py.File(merged_filename, mode="w")
        all_ds_names = set(self.in_file.keys()) | set(self.outfile.keys())
        ds_count = len(all_ds_names)
        progress_each = ds_count // min(100, round(ds_count / 500.0, 0))
        cur_i = 0
        print("[MERGING] %d + %d datasets -> %d" % (self._n_neurons, len(self.outfile), ds_count))

        for ds_name in all_ds_names:
            if ds_name not in self.outfile:
                ds = self.in_file[ds_name]
                out_arr = numpy.empty((len(ds), 3), dtype="int32")
                out_arr[:, (0, 2)] = ds
                out_arr[:, 1].fill(0)
                merged_file.create_dataset(ds_name, data=out_arr)

            elif ds_name not in self.in_file:
                ds = self.outfile[ds_name]
                out_arr = numpy.empty((len(ds), 3), dtype="int32")
                out_arr[:, (0, 1)] = ds
                out_arr[:, 2].fill(0)
                merged_file.create_dataset(ds_name, data=out_arr)

            else:
                ds = self.in_file[ds_name][:]  # Force load to mem
                other_ds = self.outfile[ds_name][:]
                out_arr = numpy.empty((len(ds) + len(other_ds), 3), dtype="int32")
                cur_index = 0
                ds_T_iter = iter(other_ds)
                other_gid, efferent_count = next(ds_T_iter, (None, 0))

                for gid, afferent_count in ds:
                    while other_gid is not None and other_gid < gid:
                        out_arr[cur_index] = (other_gid, efferent_count, 0)
                        cur_index += 1
                        other_gid, efferent_count = next(ds_T_iter, (None, 0))
                    if gid == other_gid:
                        out_arr[cur_index] = (other_gid, efferent_count, afferent_count)
                    else:
                        out_arr[cur_index] = (gid, 0, afferent_count)
                    cur_index += 1

                # Remaining - other_gid's > last gid
                while other_gid is not None:
                    out_arr[cur_index] = (other_gid, efferent_count, 0)
                    cur_index += 1
                    other_gid, efferent_count = next(ds_T_iter, (None, 0))

                merged_file.create_dataset(ds_name, data=out_arr[:cur_index])

            cur_i += 1
            if cur_i % progress_each == 0:
                self.logger.info("Merged %5d /%5d [%3d%%]",
                                 cur_i, ds_count, 100 * cur_i // ds_count)

        merged_file.close()
        self.logger.info("Merging complete.")


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
        assert self.outfile is not None, "Please run the transposition"
        in_file = self.in_file if not reverse else self.outfile
        out_file = self.outfile if not reverse else self.in_file
        errors = 0

        problematic_grps = set()
        missing_points = []
        for name, group in iteritems(in_file):
            for id1, cnt in group:
                if cnt == 0:
                    continue
                try:
                    ds = out_file["a" + str(id1)]
                except KeyError:
                    problematic_grps.add(id1)
                    continue

                id2 = int(name[1:])
                posic = bisect_left(ds[:, 0], id2)  # logN search

                if posic == len(ds) or ds[posic, 0] != id2:
                    missing_points.append((id1, id2))
                elif ds[posic, 1] != cnt:
                    # This is really not supposed to happen
                    self.logger.error("Different values in ID {}-{}. Counts: {}. Entry: {}".format(name, id1, cnt, ds[posic]))
                    errors = 1

        if problematic_grps:
            self.logger.error("Problematic grps: %s", str(list(problematic_grps)))
            errors |= 2
        if missing_points:
            self.logger.error("Missing points: %s", str(missing_points))
            errors |= 4
        return errors

    # ----
    def check_ordered(self):
        errors = 0
        for name, group in iteritems(self.in_file):
            if not numpy.array_equal(numpy.sort(group[:,0]), group[:,0]):
                self.logger.error("Dataset %s not ordered!", name)
                errors = 1
        return errors


def run_validation():
    assert cter.check_ordered() == 0, "Order errors were found"

    print("Validating...")
    assert cter.validate() == 0

    print("Reverse validating...")
    assert cter.validate(reverse=True) == 0


_doc = """
Usage:
  nrn_summary transpose <input-file> [-o=<output-file>] [--sparse] [-vv]
  nrn_summary tmerge <input-file> [-o=<output-file>] [--sparse] [-vv]
  nrn_summary -h

Options:
  -h                Show help
  -o=<output-file>  By default creates input_name.T (transposed) or input_name.merged (tmerge)
  --sparse          Runs the sparse algorithm, which saves memory and might be faster on highly sparse datasets
  -vv               Verbose mode (-v for info, -vv for debug) 
"""

if __name__ == "__main__":
    args = docopt(_doc)
    cter = NrnCompleter(args["<input-file>"], verbose=args["-v"])

    if args["transpose"]:
        cter.create_transposed(args["-o"], sparse=args["--sparse"])
    elif args["tmerge"]:
        cter.create_transposed(sparse=args["--sparse"])
        cter.merge(args["-o"])
