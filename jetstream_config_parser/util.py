import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path

import cattr

converter = cattr.Converter()


@contextmanager
def TemporaryDirectory():
    name = Path(tempfile.mkdtemp())
    try:
        yield name
    finally:
        shutil.rmtree(name)
