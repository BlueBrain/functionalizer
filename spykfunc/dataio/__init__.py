from ..utils import get_logger
logger = get_logger(__name__)

try:
    import morphotool
except ImportError:
    morphotool = False
    logger.warning("Morphotool is not available. Export to h5 won't be possible")

