from spykfunc import functionalizer as fz, commands
from os import path

BASE_DIR = path.expanduser("~/dev/TestData/circuitBuilding_1000neurons")
DATA_31K = path.expanduser("~/scratch/circuit_31k")
#DATA_31K = "/gpfs/bbp.cscs.ch/scratch/gss/leite/circuit_31k"
DATA_2M = "/gpfs/bbp.cscs.ch/scratch/gss/leite/circuit_2M"

print("Path for touches: " + path.join(DATA_31K, "circuit_spark/touches.*"))
args = (
    path.join(BASE_DIR, "recipe/builderRecipeAllPathways.xml"),  # recipe_file
    path.join(DATA_31K, "circuit.mvd3"),                          # mvd_file
    path.join(BASE_DIR, "morphologies/h5"),                      # morpho_dir
    path.join(DATA_31K, "circuit_spark/touches.*")               # touch_files
)

if __name__ == "__main__":
    opts = commands.arg_parser.parse_args(args)
    fuzer = fz.session(opts)
    fuzer.process_filters()
