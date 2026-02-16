"""
gemaakt voor LIWO door  David Haasnoot (d.haasnoot@hkv.nl)
Juni, 2025

# DOEL
Bevat functies voor interactie met de API, worden gebruikt door andere scripts.
"""

import shutil
import sys
from pathlib import Path

import pandas as pd
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
        if scenario.is_dir():
            check_1 = int(scenario.name) in copy_ids
            check_2 = not (to_dir / scenario.name).exists()
            if check_1 and check_2:
                shutil.copytree(scenario, to_dir / scenario.name)

    copied_dirs = to_dir.glob("*")
    copied_scenarios = [int(file.name) for file in copied_dirs]
    missing_scenarios = set(copy_ids).difference(copied_scenarios)
    print(f"Missing {len(missing_scenarios)} scenarios")
    df = pd.DataFrame(list(missing_scenarios), columns=['ids'])
    df.to_csv(current_dir / "missing_ids.csv")



if __name__ == "__main__":
    main(sys.argv)
