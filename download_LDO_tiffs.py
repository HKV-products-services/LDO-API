"""
gemaakt voor LIWO door  David Haasnoot (d.haasnoot@hkv.nl)
"""

from pathlib import Path
import zipfile
from export_LDO import (
    get_file_url,
    download_tif,
)
from update_local_bulk_LDO import haal_scenarios_op, haal_token_op
import pandas as pd
import dotenv
from tqdm import tqdm

import logging


def export_tiffs(beschikbare_scenario_ids: list, work_dir: Path, headers: dict) -> None:
    names_tiffs_ssm = [
        "Mortality.tif",
        "Total_damage.tif",
        "Total_victims.tif",
        "Total_affected.tif",
    ]
    calculated_postfix_names_tiffs = [
        "rise_period.tiff",
        "max_velocity.tiff",
        "rate_of_rise.tiff",
        "arrival_times.tiff",
    ]
    export_dir = work_dir / "downloaded_tiffs"
    missing_values = {}
    try:
        for scenario_id in tqdm(beschikbare_scenario_ids):
            calculated_names_tiffs = [
                f"scenario_{scenario_id}_{name_suffix}"
                for name_suffix in calculated_postfix_names_tiffs
            ]
            all_names_tiffs = names_tiffs_ssm + calculated_names_tiffs
            for name in all_names_tiffs:
                status_code, url = get_file_url(scenario_id, name, headers)
                if status_code == 200:
                    try:
                        download_tif(url, name, scenario_id, export_dir)
                    except ConnectionError as e:
                        logger.error(f"Connection error during download: {e}")
                        # try again
                        try:
                            download_tif(url, name, scenario_id, export_dir)
                        except ConnectionError as e:
                            logger.error(
                                f"Connection error during download (2nd try): {e}, {url}, {scenario_id}"
                            )
                            continue

                else:
                    logger.info(
                        f"Failed to download {name} for scenario {scenario_id}: {url}"
                    )
                    if scenario_id in missing_values:
                        missing_values[scenario_id].append(name)
                    else:
                        missing_values[scenario_id] = [name]
    except Exception as e:
        logger.error(f"Error during download: {e}")

    # still write the missing values to a csv & zip
    missing_values_df = pd.DataFrame.from_dict(missing_values, orient="index")
    missing_values_df.to_csv(export_dir / "missing_values.csv")
    with zipfile.ZipFile(work_dir / "downloaded_tiffs.zip", "w") as zipf:
        for file in export_dir.iterdir():
            zipf.write(file, file.name)


if __name__ == "__main__":
    current_dir = Path.cwd()

    # Set up basic logger
    log_file = current_dir / "log_tiff.txt"
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger()

    # zet de LDO api key in de .env file
    if dotenv.load_dotenv():
        environmental_variables = dotenv.dotenv_values()
        LDO_api_key = environmental_variables["LDO_api_key"]
    headers = haal_token_op(LDO_api_key)

    logger.info("haal scenarios op")
    beschikbare_scenario_ids = haal_scenarios_op(
        maximum=10_000, headers=headers
    )  # misschien later meer dan 10_000?

    logger.info("Vergelijk scenarios")  # toe te voegen
    nieuwe_scenarios = beschikbare_scenario_ids

    logger.info("Start export scenarios")
    lst_zips_nieuwe_export = export_tiffs(
        nieuwe_scenarios,
        current_dir,
        headers=headers,
    )
