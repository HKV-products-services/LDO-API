"""
gemaakt voor LIWO door  David Haasnoot (d.haasnoot@hkv.nl)
Juni, 2025

# DOEL
Bevat functies voor interactie met de API, worden gebruikt door andere scripts.
"""

import sys
from pathlib import Path
import pandas as pd


def main(args):
    if len(sys.argv) > 1:
        download_dir = Path(sys.argv[1])
    else:
        current_dir = Path.cwd()
        download_dir = current_dir / "downloaded_tiffs"

    scenarios = list(download_dir.glob("*"))

    d = {}
    for scenario in scenarios:
        d[scenario.name] = []
        for file in scenario.glob("*"):
            if file.is_file():
                file_size = file.stat().st_size
                d[scenario.name].append(file_size.name)
        d[scenario.name] = ", ".join(d[scenario.name])

    df = pd.DataFrame.from_dict(d, orient="index", columns=["files"])
    df.to_csv(current_dir / "file_sizes.csv")


if __name__ == "__main__":
    main(sys.argv)
