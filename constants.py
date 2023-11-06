import os

PYXET_REPO = os.getenv("PYXET_REPO", "xet://xdssio/xethub-py-3/main")
GITXET_REPO = os.getenv("GITXET_REPO", "xet://xdssio/xethub-git-3/main")
LAKEFS_REPO = os.getenv("LAKEFS_REPO", "lakefs://benchmarks/main")
S3_BUCKET = os.getenv("S3_BUCKET", "benchmarks-uploads")
