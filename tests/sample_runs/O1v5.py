from spykfunc import session
from os import path

CIRCUIT_DIR = path.expanduser("~/dev/TestData/O1.v5")

args = (
    path.join(CIRCUIT_DIR, "builderRecipeAllPathways.xml"),   # recipe file
    path.join(CIRCUIT_DIR, "nodes.h5"),                       # circuit file
    path.join(CIRCUIT_DIR, "touches/touchesData.*.parquet"),  # touch files
    "--driver-memory 4G"
)

if __name__ == "__main__":
    fuzer = session(*args)
    fuzer.process_filters()
    fuzer.export_results()
