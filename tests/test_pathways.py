"""Test pathway calculation and if it matches with expectations
"""

from io import StringIO
from itertools import product
import pandas as pd
import sparkmanager as sm

from fz_td_recipe import Recipe
from spykfunc.circuit import Circuit


MTYPES = ["BAR", "BAZ"]
REGIONS = ["NONE", "SOME", "MANY"]

PROPERTY = (
    '<mTypeRule toMType="{}" toRegion="{}" bouton_reduction_factor="0.9" pMu_A="0.9" p_A="0.9" />'
)

SOME_PROPERTIES = """\
<?xml version="1.0"?>
<blueColumn>
  <ConnectionRules>
{}
  </ConnectionRules>
</blueColumn>
""".format(
    "\n".join(PROPERTY.format(m, r) for m, r in product(MTYPES, REGIONS))
)


class _MockNodes:
    mtype_values = MTYPES
    region_values = REGIONS


class _MockDf:
    def count(self):
        return 0


class _MockEdges:
    df = _MockDf()
    metadata = None
    input_size = -1


def test_pathway_generation(fz, monkeypatch):
    r = Recipe(StringIO(SOME_PROPERTIES))

    cr = r.connection_rules
    matrix = cr.to_matrix({"toMType": MTYPES, "toRegion": REGIONS})

    with monkeypatch.context() as m:

        def mock_build(_self, _touches, checkpoint=False):
            return None

        m.setattr(Circuit, "build_circuit", mock_build)
        c = Circuit(_MockNodes(), _MockNodes(), _MockEdges(), "/")

    df = pd.DataFrame.from_records(
        data=[
            (m, r, MTYPES[m], REGIONS[r])
            for m, r in product(range(len(MTYPES)), range(len(REGIONS)))
        ],
        columns=["dst_mtype_i", "dst_region_i", "dst_mtype", "dst_region"],
    )

    with c.pathways(["toMType", "toRegion"]):
        w_pathways = c.with_pathway(sm.createDataFrame(df)).toPandas()
        for pathway, mtype, region in zip(
            w_pathways["pathway_i"],
            w_pathways["dst_mtype"],
            w_pathways["dst_region"],
        ):
            rule_i = matrix[pathway]
            assert mtype == cr[rule_i].toMType
            assert region == cr[rule_i].toRegion
