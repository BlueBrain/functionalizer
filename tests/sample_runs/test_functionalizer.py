from spykfunc import functionalizer as fz, commands
from os import path

BASE_DIR = path.join(path.dirname(__file__), "circuit_1000n")
args = (
    path.join(BASE_DIR, "builderRecipeAllPathways.xml"),     # recipe_file
    path.join(BASE_DIR, "circuit.mvd3"),                     # mvd_file
    path.join(BASE_DIR, "morphologies/h5"),                  # morpho_dir
    path.join(BASE_DIR, "touches/*.parquet"),    # touch_files
)

if __name__ == "__main__":
    opts = commands.arg_parser.parse_args(args)
    fuzer = fz.session(opts)
    fuzer.process_filters()
    fuzer.export_results()
