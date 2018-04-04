from spykfunc import functionalizer as fz, commands
from os import path

BASE_DIR = path.expanduser("~/dev/TestData/S1HL-200um")
args = (
    path.join(BASE_DIR, "builderRecipeAllPathways.xml"),  # recipe_file
    path.join(BASE_DIR, "circuit.mvd3"),                         # mvd_file
    BASE_DIR,                                                    # morpho_dir
    path.join(BASE_DIR, "touches/touchesData.*.parquet")         # touch files
)

if __name__ == "__main__":
    opts = commands.arg_parser.parse_args(args)
    fuzer = fz.session(opts)
    fuzer.process_filters()
