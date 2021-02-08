from spykfunc import functionalizer as fz, commands
from os import path

CIRCUIT_v5_30k = path.expanduser("~/scratch/O0.v5")

args = (
    path.join(CIRCUIT_v5_30k, "builderRecipeAllPathways2.xml"),  # recipe file
    path.join(CIRCUIT_v5_30k, "nodes.h5"),                       # circuit file
    path.join(CIRCUIT_v5_30k, "morphologies/h5"),                # morpho dir
    path.join(CIRCUIT_v5_30k, "touches/touchesData.*.parquet")   # touch files
)

if __name__ == "__main__":
    opts = commands.arg_parser.parse_args(args)
    fuzer = fz.session(opts)
    fuzer.process_filters()
    fuzer.export_results()
