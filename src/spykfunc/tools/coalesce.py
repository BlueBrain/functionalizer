"""Convert a multi-parquet circuit into a single file."""

import glob
import os
import shutil
import sys
import tempfile

import sparkmanager as sm


def run():
    """Entry point."""
    try:
        infiles, outfile = sys.argv[1:]  # pylint: disable=unbalanced-tuple-unpacking
    except ValueError:
        print(f"usage: {os.path.basename(sys.argv[0])} directory parquetfile")
        sys.exit(1)

    tmpname = tempfile.mktemp()

    sm.create("foo")
    sm.read.load(infiles).coalesce(1).write.parquet(tmpname)

    (filename,) = glob.glob(f"{tmpname}/*.parquet")

    shutil.move(filename, outfile)
    shutil.rmtree(tmpname)


if __name__ == "__main__":
    run()
