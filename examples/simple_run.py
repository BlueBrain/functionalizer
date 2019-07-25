from os import path
try:
    from spykfunc import session as fz_session
except ImportError as e:
    raise ImportError("Can't import spykfunc. Make sure you install it first (inc. develop) or update PYTHONPATH")

CIRCUIT_DIR = path.join(path.dirname(__file__), "../tests/circuit_1000n")
args = (
    path.join(CIRCUIT_DIR, "builderRecipeAllPathways.xml"),  # recipe_file
    path.join(CIRCUIT_DIR, "circuit.mvd3"),                  # circuit_file
    path.join(CIRCUIT_DIR, "touches/touchesData.*.parquet"), # touch files
)

if __name__ == "__main__":
    """ This simple use of the API is roughly equivalent to launching spykfunc from the terminal
    """
    fuzer = fz_session(*args)
    res = fuzer.process_filters()
    assert res == 0
    fuzer.export_results()
