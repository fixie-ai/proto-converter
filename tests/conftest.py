import sys
from pathlib import Path

# Make generated proto modules importable as test_api.api_pb2 / test_internal.internal_pb2.
sys.path.insert(0, str(Path(__file__).parent / "gen"))
