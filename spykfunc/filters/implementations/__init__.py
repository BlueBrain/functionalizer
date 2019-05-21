"""Reference implementations of filters
"""
from spykfunc.recipe import GenericProperty

class Seeds(GenericProperty):
    """Container to store seeds
    """
    _supported_attrs = {'recipeSeed', 'columnSeed', 'synapseSeed'}
    recipeSeed = 0
    columnSeed = 0
    synapseSeed = 0

    @classmethod
    def load(cls, xml):
        """Load seeds from XML structure.
        """
        seed_info = xml.find("Seeds")
        if hasattr(seed_info, "items"):
            infos = {k: v for k, v in seed_info.items()}
            return cls(**infos)
        return cls()
