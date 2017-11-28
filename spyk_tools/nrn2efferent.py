"""
Program which takes a nrn.h5 file and builds the nrn_efferent.h5
"""
from __future__ import print_function
import sys
import h5py
import numpy
from docopt import docopt
from future.builtins import range
from future.utils import iteritems
from collections import defaultdict
from bisect import bisect_left
import logging


class NrnConverter(object):
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

    # --
    def create_efferent(self, output_filename=None):
        """
        Create a transposed version of the h5 file, datasets ("columns") become rows, rows become datasets
        """
        self.outfile = h5py.File(output_filename or "nrn_efferent.h5", "w")
        id_limit = self.max_id + 1
        print("[TRANSPOSING] %d touches in blocks of %dx%d" %
              (id_limit, self._GROUP_SIZE, self._GROUP_SIZE))

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
                group_max_len = (id_stop_2 - id_start_2) * 10  # Unlike nrn_summary, many entries per connection

                for i, post_gid in enumerate(postgids):
                    ds_name = "a"+str(post_gid)
                    ds = self.in_file[ds_name]
                    cur_offset = sub_offset[i]
                    pre_gids = ds[cur_offset:cur_offset+group_max_len, 0].astype("uint32")

                    # Find the position using numpy to load only the required data
                    last_elem_i = numpy.searchsorted(pre_gids, id_stop_2)
                    data = ds[cur_offset:cur_offset+last_elem_i]
                    pre_gids = pre_gids[:last_elem_i]

                    # post_gid change detection
                    changed_gid_pos = numpy.where(pre_gids[1:] != pre_gids[:-1])[0] + 1

                    # Update gid
                    data[:, 0].fill(float(post_gid))

                    cur_group_start_i = 0
                    for change_pos in changed_gid_pos:
                        pre_gid = pre_gids[change_pos - 1]
                        self._outbuffers["a"+str(pre_gid)].append(data[cur_group_start_i:change_pos])
                        self._outbuffer_entries += (change_pos - cur_group_start_i)
                        cur_group_start_i = change_pos

                    # Save remaining group
                    pre_gid = pre_gids[last_elem_i - 1]
                    self._outbuffers["a" + str(pre_gid)].append(data[cur_group_start_i:last_elem_i])
                    self._outbuffer_entries += (cur_group_start_i - last_elem_i)

                    # Save offset of entries processed so far
                    sub_offset[i] = cur_offset + last_elem_i

                if self._outbuffer_entries > self._MAX_OUTBUFFER_LEN:
                    self._flush_outbuffers()

        self.logger.debug("Final buffer flush")
        self._flush_outbuffers(final=True)
        self.logger.info("Transposing complete")

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
                    self.outfile.create_dataset(ds_name, data=merged_data, chunks=(1000, 19), maxshape=(None, 19))
            else:
                ds = self.outfile[ds_name]
                cur_length = len(ds)
                ds.resize(cur_length + len(merged_data), axis=0)
                ds[cur_length:] = merged_data

        self._outbuffers = defaultdict(list)
        self._outbuffer_entries = 0

    # --
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
        errors = 0

        problematic_grps = set()
        missing_points = []
        n_items = len(in_file.keys())
        count = 0

        for name, group in iteritems(in_file):
            print(".", end="", file=sys.stderr)
            count +=1
            if count % (n_items//20) == 0:
                print("\n{:.1f}%".format(100.0*count/n_items), end="")

            prev_id = None
            id2 = int(name[1:])

            for row in group:
                id1 = int(row[0])
                field1 = row[1]

                if prev_id == id1:
                    posic += 1
                else:
                    try:
                        ds = out_file["a" + str(id1)]
                    except KeyError:
                        problematic_grps.add(id1)
                        continue
                    posic = bisect_left(ds[:, 0], float(id2))  # logN search
                    prev_id = id1

                if posic == len(ds) or int(ds[posic, 0]) != id2:
                    missing_points.append((id1, id2))
                elif ds[posic, 1] != field1:
                    # This is really not supposed to happen
                    self.logger.error("Different values in ID {}-{}. {} vs {}".format(name, id1, row, ds[posic]))
                    errors = 1

        if problematic_grps:
            self.logger.error("Problematic grps: %s", str(list(problematic_grps)))
            errors |= 2
        if missing_points:
            self.logger.error("Missing points: %s", str(missing_points))
            errors |= 4
        return errors


def run_validation():
    print("Validating...")
    assert cter.validate() == 0
    # print("Reverse validating...")
    # assert cter.validate(reverse=True) == 0


_doc = """
Usage:
  nrn2efferent <input-file> [-o=<output-file>] [-vv]
  nrn2efferent -h

Options:
  -h                Show help
  -o=<output-file>  By default creates nrn_efferent.h5
  -vv               Verbose mode (-v for info, -vv for debug) 
"""

if __name__ == "__main__":
    args = docopt(_doc)
    cter = NrnConverter(args["<input-file>"], verbose=args["-v"])
    cter.create_efferent(args["-o"])
    cter.validate()