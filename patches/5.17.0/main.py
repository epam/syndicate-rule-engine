from common import main as patch_main

from patch_fingerprints import PatchFingerprints
from patch_jobs import PatchJobs


if __name__ == "__main__":
    patches = [
        PatchFingerprints(),
        PatchJobs(),
    ]
    patch_main(patches)
