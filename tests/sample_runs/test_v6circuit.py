from spykfunc import functionalizer as fz, commands
from os import path

BASE_DIR = path.expanduser("~/dev/TestData/S1HL-200um")
args = (
    path.join(BASE_DIR, "builderRecipeAllPathways.xml"),  # recipe file
    path.join(BASE_DIR, "nodes.h5"),                      # circuit file
    BASE_DIR,                                             # morpho dir
    path.join(BASE_DIR, "touches/touchesData.*.parquet")  # touch files
)

if __name__ == "__main__":
    opts = commands.arg_parser.parse_args(args)
    fuzer = fz.session(opts)
    fuzer.process_filters()
