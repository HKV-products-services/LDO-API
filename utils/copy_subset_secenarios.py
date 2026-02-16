"""
gemaakt voor LIWO door  David Haasnoot (d.haasnoot@hkv.nl)
Juni, 2025

# DOEL
Bevat functies voor interactie met de API, worden gebruikt door andere scripts.
"""

import shutil
import sys
from pathlib import Path
from copy_ids import copy_ids


def main(args):
    if len(sys.argv) > 1:
        download_dir = Path(sys.argv[1])
        to_dir = Path(sys.argv[2])
    else:
        current_dir = Path.cwd()
        download_dir = current_dir / "downloaded_tiffs"
        to_dir = current_dir / "subset_tiffs"

    scenarios = list(download_dir.glob("*"))

    for scenario in scenarios:
        if int(scenario.name) in copy_ids:
            shutil.copytree(scenario, to_dir / scenario.name)


if __name__ == "__main__":
    main(sys.argv)
