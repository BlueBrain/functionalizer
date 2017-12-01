"""
Program which takes a nrn_summary with only post_neuron touch count and calculates the pre_neuron counterpart
"""
from __future__ import print_function, division
import h5py
from array import array
import numpy
from docopt import docopt
from future.builtins import range
from future.utils import iteritems
from collections import defaultdict
from bisect import bisect_left
import logging
from progress.bar import Bar
from itertools import islice


class NrnCompleter(object):
    # Please use base2 vals
    _GROUP_SIZE = 8 * 1024
    _MAX_OUTBUFFER_LEN = 10 * 1024**2  # 1M entries ~ 8MB mem
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

    # --
    def create_transposed(self, output_filename=None):
        """
        Create a transposed version of the h5 file, datasets ("columns") become rows, rows become datasets
        :param sparse: If the h5 file matrix is not dense (case of touches) we can use sparse to avoid creating an \
        intermediate matrix structure and use a simple algorithm.
        """
        output_filename = output_filename or self._in_filename + ".T"
        self.outfile = h5py.File(output_filename, "w")
        id_limit = self.max_id + 1

        print("[TRANSPOSING] %d neurons in blocks of %dx%d (mode: Sparse)" %
              (id_limit, self._GROUP_SIZE, self._GROUP_SIZE))

        bar = Bar("Progress", max=(round(id_limit / self._GROUP_SIZE)) ** 2)
        bar.start()

        # For loop just to control Datasets in the block (outer gid)
        for ds_start_i in range(0, id_limit, self._GROUP_SIZE):
            ds_stop_i = min(id_limit, ds_start_i + self._GROUP_SIZE)
            postgids = array("i")
            sub_offset = array("i")

            # Init structures for the current group block
            for post_gid in range(ds_start_i, ds_stop_i):
                if ("a" + str(post_gid)) not in self.in_file:
                    continue
                postgids.append(post_gid)
                sub_offset.append(0)

            # For loop to control the Datasets' rows in the block (inner gid)
            for rec_start_i in range(0, id_limit, self._GROUP_SIZE):
                _array_dict_sub = self.transpose_block(postgids, rec_start_i, id_limit, sub_offset)
                self._store_block(_array_dict_sub)
                bar.next()

        bar.finish()
        print("Final buffer flush...")
        self._flush_outbuffers(final=True)
        print("Transposing complete")

    # --
    def transpose_block(self, postgids, rec_start_i, id_limit, sub_offset):
        rec_stop_i = min(id_limit, rec_start_i + self._GROUP_SIZE)
        last_section = rec_stop_i == id_limit  # The max inner GID isn't necessarily the max out
        _array_dict = defaultdict(lambda: array("i"))
        _BLOCK_SIZE = 512  # 4*2*512 = 4K

        for i, post_gid in enumerate(postgids):
            ds_name = "a" + str(post_gid)
            ds = self.in_file[ds_name]
            cur_offset = sub_offset[i]

            if last_section:
                data = ds[cur_offset:]
                for row in data:
                    _array_dict[row[0]].extend([post_gid, row[1]])
                sub_offset[i] = cur_offset + len(data)
            else:
                while True:
                    # Lets read blocks, instead of the full length
                    data = ds[cur_offset: cur_offset + _BLOCK_SIZE]
                    max_row_i = numpy.searchsorted(data[:, 0], rec_stop_i)
                    sub_offset[i] = cur_offset + max_row_i

                    for row in data[:max_row_i]:
                        _array_dict[row[0]].extend([post_gid, row[1]])

                    # We can stop reading blocks if our max value is found in the middle of the buffer
                    if max_row_i < _BLOCK_SIZE:
                        break

        return _array_dict

    # --
    def _store_block(self, _array_dict):
        for ds, arr in iteritems(_array_dict):   # type: int, array
            arr_np = numpy.frombuffer(arr, dtype="int32").reshape((-1, 2))  # type: numpy.ndarray
            self._outbuffers["a" + str(ds)].append(arr_np)
            self._outbuffer_entries += len(arr_np)

            if self._outbuffer_entries > self._MAX_OUTBUFFER_LEN:
                self._flush_outbuffers()

    # --
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

    # --
    def merge(self, second_file=None, merged_filename=None):
        """
        Merger of both forward and reverse matrixes (afferent and efferent touch count)
        :param second_file: The file to merge with. (defaults to input_filename.T)
        :param merged_filename: The name of the output merged file
        """
        if not self.outfile:
            self.outfile = h5py.File(second_file or self._in_filename + ".T", "r")

        merged_filename = merged_filename or self._in_filename + ".merged"
        merged_file = h5py.File(merged_filename, mode="w")
        all_ds_names = set(self.in_file.keys()) | set(self.outfile.keys())
        ds_count = len(all_ds_names)

        print("[MERGING] %d + %d datasets -> %d" % (self._n_neurons, len(self.outfile), ds_count))
        i = 0
        bar = Bar("Progress", max=ds_count//100)
        bar.start()

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
                ds1 = self.in_file[ds_name][:]
                ds2 = self.outfile[ds_name][:]

                only_ds1 = numpy.isin(ds1[:, 0], ds2[:,0], assume_unique=True, invert=True)
                len_only_ds1 = numpy.count_nonzero(only_ds1)
                part1 = numpy.zeros((len_only_ds1, 3), dtype="int32")
                part1[:, [0, 2]] = ds1[only_ds1]

                only_ds2 = numpy.isin(ds2[:, 0], ds1[:, 0], assume_unique=True, invert=True)
                len_only_ds2 = numpy.count_nonzero(only_ds2)
                part2 = numpy.zeros((len_only_ds2, 3), dtype="int32")
                part2[:, [0, 1]] = ds2[only_ds2]

                common_len = numpy.size(only_ds1) - len_only_ds1
                common = numpy.empty((common_len, 3), dtype="int32")
                common[:, [0, 1]] = ds2[~only_ds2]
                common[:, 2] = ds1[~only_ds1][:, 1]

                merged = numpy.concatenate((part1, part2, common))
                merged.sort(0)
                merged_file.create_dataset(ds_name, data=merged)

            i += 1
            if i % 100 == 0:
                bar.next()
        bar.finish()

        merged_file.close()
        print("\nMerging complete.")

    # ----
    @staticmethod
    def add_meta(filename, infodic):
        f = h5py.File(filename)
        i = f.require_dataset("info", [], dtype="int32")
        i.attrs.update(infodic)
        f.close()

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
        problematic_grps = set()
        missing_points = []
        errors = 0
        bar = Bar("Progress", max=20)
        bar.start()

        for name, group in islice(iteritems(in_file), 20):
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
                    self.logger.error("Different values in ID {}-{}. Counts: {}. Entry: {}"
                                      .format(name, id1, cnt, ds[posic]))
                    errors = 1
            bar.next()
        bar.finish()

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
  nrn_summary transpose <input-file> [-o=<output-file>] [-vv]
  nrn_summary merge <input-file> [-a=<other-file>] [-o=<output-file>] [-vv]
  nrn_summary tmerge <input-file> [-o=<output-file>] [-vv]
  nrn_summary -h

Options:
  -h                Show help
  -o=<output-file>  By default creates input_name.T (transposed) or input_name.merged (tmerge)
  -a=<other-file>   The file to merge with in merge-only mode (by default uses input-file.T)
  --sparse          Runs the sparse algorithm, which saves memory and might be faster on highly sparse datasets
  -vv               Verbose mode (-v for info, -vv for debug) 
"""

if __name__ == "__main__":
    args = docopt(_doc)
    cter = NrnCompleter(args["<input-file>"], verbose=args["-v"])

    if args["transpose"]:
        cter.create_transposed(args["-o"])
    elif args["merge"]:
        cter.merge(args["-a"], merged_filename=args["-o"])
    elif args["tmerge"]:
        cter.create_transposed()
        cter.merge(merged_filename=args["-o"])
