"""
gemaakt voor LIWO door  David Haasnoot (d.haasnoot@hkv.nl)
Juni, 2025

# DOEL
Bevat functies voor interactie met de API, worden gebruikt door andere scripts.
"""

import sys
from pathlib import Path
from tqdm import tqdm


def main(args):
    if len(sys.argv) > 1:
        download_dir = Path(sys.argv[1])
    else:
        current_dir = Path.cwd()
        download_dir = current_dir / "downloaded_tiffs"

    scenarios = list(download_dir.glob("*"))

    wanted_list = [
        "arrival_times.tiff",
        "max_velocity.tiff",
        "max_waterdepth.tiff",
        "Mortality.tif",
        "rate_of_rise.tiff",
        "rise_period.tiff",
        "Total_affected.tif",
        "Total_damage.tif",
        "Total_victims.tif",
    ]
    for scenario in tqdm(scenarios):
        for file in scenario.glob("*"):
            if file.is_file():
                if file.name not in wanted_list:
                    file.unlink()


if __name__ == "__main__":
    main(sys.argv)
