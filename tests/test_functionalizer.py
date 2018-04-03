from spykfunc import functionalizer as fz, commands
from os import path
import sys

BASE_DIR = path.join(path.dirname(__file__), "circuit_1000n")
args = (
    path.join(BASE_DIR, "builderRecipeAllPathways.xml"),     # recipe_file
    path.join(BASE_DIR, "circuit.mvd3"),                     # mvd_file
    path.join(BASE_DIR, "morphologies/h5"),                  # morpho_dir
    path.join(BASE_DIR, "BlueDetector_output/*.parquet"),    # touch_files
)

if __name__ == "__main__":
    opts = commands.arg_parser.parse_args(args)
    fuzer = fz.session(opts)
    res = fuzer.process_filters()
    res = 0
    if res:
        sys.exit(res)
    fuzer.export_results()
