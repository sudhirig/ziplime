import dataclasses
from typing import Callable


@dataclasses.dataclass
class BundleCore:
    bundles: dict
    register: Callable
    ingest: Callable
    load: Callable
    clean: Callable
