"""BARRAGE — adaptive multi-vector stress-testing framework.

Vectors: httpflood, http2reset, slowloris, slowpost, tcpexhaust, udpflood.
Engine:  open-loop pacing, multiprocess workers, token-bucket rate shaping.
Brain:   online anomaly detection + collapse finder + vector recommender.

Use ONLY against systems you own or are explicitly authorized to test.
"""

__version__ = "0.2.0"

from .config import AttackConfig, Target, LoadModel  # noqa: F401
from .metrics import WorkerStats  # noqa: F401
