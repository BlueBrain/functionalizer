from spykfunc import session
from os import path

CIRCUIT_DIR = path.expanduser("~/dev/TestData/O1.v5")

args = (
    path.join(CIRCUIT_DIR, "builderRecipeAllPathways.xml"), # recipe_file
    path.join(CIRCUIT_DIR, "circuit.mvd3"),                 # mvd_file
    path.join(CIRCUIT_DIR, "touches/touches.0"),            # touch_file0
    "--driver-memory 4G"
)

if __name__ == "__main__":
    fuzer = session(*args)
    fuzer.process_filters()
    fuzer.export_results()
