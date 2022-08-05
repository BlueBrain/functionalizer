from spykfunc import functionalizer as fz, commands
from os import path

BASE_DIR = path.join(path.dirname(__file__), "..", "circuit_1000n")
args = (
    path.join(BASE_DIR, "builderRecipeAllPathways.xml"),  # recipe file
    path.join(BASE_DIR, "nodes.h5"),  # circuit file
    path.join(BASE_DIR, "morphologies/h5"),  # morpho dir
    path.join(BASE_DIR, "touches/*.parquet"),  # touch files
)

if __name__ == "__main__":
    opts = commands.arg_parser.parse_args(args)
    fuzer = fz.session(opts)
    fuzer.process_filters()
    fuzer.export_results()
