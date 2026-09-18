import os
import sys
import time

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
_PIPELINE = os.path.join(_ROOT, "pipeline")

for sub in ["", "common", "ingestion", "processing", "features", "validation", "recon"]:
    p = os.path.join(_PIPELINE, sub) if sub else _PIPELINE
    if p not in sys.path:
        sys.path.insert(0, p)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
