"""
gemaakt voor LIWO door David Haasnoot (d.haasnoot@hkv.nl)
Jun, 2025


# DOEL
Download de bulk export en voegt dit toe aan een bestaande export.

"""

import logging
from pathlib import Path
import zipfile
from export_LDO import (
    get_scenario_list,
    combine_functions_start_export,
    combine_functions_download_export,
)
import pandas as pd
import requests
import dotenv
import shutil

"""
Stappen plan voor het aanmaken van een api key.
- Login op https://www.overstromingsinformatie.nl/, dit loop via bij12.
- Ga naar https://www.overstromingsinformatie.nl/auth/
- Scrol naar beneden onder `V1` ,dan `POST auth/v1/personalapikeys` (in het groen) en klik deze open.
- klik op `try it out` en vervang body met:
```json
{
  "scope": "*:readwrite",
  "name": "personalAPIkeyLIWOexport...",
  "expiry_date": "2029-12-31T23:59:59.037Z",
  "revoked": false
}
```
- Pas de datum en name aan waar nodig
- klik op `Excecute` in het blauw
- scrol naar beneden (maar nog in het zelfde groene vak), in de onderste 2 zwarte vlakken zie je response code 201 als het gelukt is.
- In de response body staat: 
```json
{
  "prefix": "xxxxx",
  "scope": "*:readwrite",
  "name": "personalAPIkeyLIWOexportTest2",
  "expiry_date": "2029-12-31T23:59:59.037000Z",
  "created": "2025-06-04T09:18:11.559373Z",
  "revoked": false,
  "last_used": null,
  "key": "xxxxxx.yyyyyyyyyyyyyyy",
  "message": "Please store the key somewhere safe, you will not be able to see it again."
}
```
- Bewaar die hele `'key'` in een bestand die `.env` heet, zie `example.env` voor het formaat.
meer informatie staat onderaan of op de docs: https://www.overstromingsinformatie.nl/api/v1/docs

"""


def haal_token_op(api_key: str) -> dict:
    """Haal de access token op voor www.overstromingsinformatie.nl gegeven de api key"""
    url_auth = "https://www.overstromingsinformatie.nl/auth/v1/token/"
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    response = requests.post(
        url=url_auth, headers=headers, json={"tenant": 1}, auth=("__key__", api_key)
    )
    id_token = response.json()["access"]
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {id_token}",
    }
    return headers


def haal_scenarios_op(maximum: int, headers: dict) -> list:
    """ "Haal de scenario ids op"""
    limit_per_request = 100
    offset = 0
    beschikbare_scenario_ids = get_scenario_list(
        offset, limit_per_request, maximum, headers
    )
    return beschikbare_scenario_ids


def vergelijke_nieuwe_en_huidige(
    combined_file: Path, beschikbare_scenario_ids: list
) -> tuple[list, list, pd.DataFrame]:
    """Open de map Combined file, lees het huidige meta data bestand uit en vergelijk de ids hier in met de opgehaalde lijst"""
    with zipfile.ZipFile(combined_file, "r") as archive:
        name_list = archive.namelist()
        file_extensions = [name.split(".")[-1] for name in name_list]
        excel_file = name_list[file_extensions.index("xlsx")]
        with archive.open(excel_file) as f_open:
            df_current_local_LDO = pd.read_excel(f_open, index_col=0)

    huidige_scenarios = set(df_current_local_LDO.index)
    nieuwe_scenarios = list(set(beschikbare_scenario_ids).difference(huidige_scenarios))
    verwijderde_scenarios = list(
        set(huidige_scenarios).difference(beschikbare_scenario_ids)
    )
    return verwijderde_scenarios, nieuwe_scenarios, df_current_local_LDO


def export_uit_LDO(
    nieuwe_scenarios: list, headers: dict
) -> tuple[list, dict, pd.DataFrame]:
    """Haal de nieuwe scenarios op uit het LDO, geef een lijst met de paden naar de gedownloade zips terug"""
    # Start the exports
    list_args = []  # tuple met (export_id, status, export_body) doorgeven van een functie naar de volgende
    for index, id in enumerate(nieuwe_scenarios):
        list_args.append(combine_functions_start_export(headers, index, [id]))

    # zodra alle exports zijn gestart, download ze.
    for index, id_list in enumerate(nieuwe_scenarios):
        combine_functions_download_export(headers, *list_args[index])

    #  Met de export-id's, haal de metadata dataframes van de nieuwe scenario's op
    nieuwe_scenarios_export_ids = [args[0] for args in list_args]

    return [
        current_dir / f"export_{export_id}.zip"
        for export_id in nieuwe_scenarios_export_ids
    ]


def voeg_zips_samen_verwijder_ouder(
    lst_zips_nieuwe_export: list,
    verwijderde_scenarios: list,
    df_current_local_LDO: pd.DataFrame,
    current_archive: Path,
    new_archive: Path,
) -> None:
    """
    Lees de nieuwe metadata in, combineer deze en schrijf naar een excel.
    Vervolgens worden de nieuwe scenarios toegevoegd en als laatste worden indien nodig scenarios verwijderd.
    Het nieuwe meta data bestand wordt dan toegevoegd.
    """
    skip_rows = [2, 3]
    lst_dfs_new_scenarios = []
    # open de metedata van de nieuwe scenarios
    for file in lst_zips_nieuwe_export:
        with zipfile.ZipFile(file, "r") as archive:
            name_list = archive.namelist()
            file_extensions = [name.split(".")[-1] for name in name_list]
            excel_file = name_list[file_extensions.index("xlsx")]
            with archive.open(excel_file) as f_open:
                df_new_scenarios = pd.read_excel(
                    f_open, header=1, skiprows=skip_rows, index_col=0
                )
                lst_dfs_new_scenarios.append(df_new_scenarios)

    # stel de nieuwe meta data samen
    df_new_scenarios = pd.concat(lst_dfs_new_scenarios, axis=0)
    df_final = pd.concat([df_current_local_LDO, df_new_scenarios], axis=0)

    df_final = df_final.drop(index=verwijderde_scenarios)
    temp_excel_output_name = "merged_excel_temp_update.xlsx"
    excel_name_in_zip = "merged_excel.xlsx"
    excel_output_dir = current_archive.parent / temp_excel_output_name
    df_final.to_excel(temp_excel_output_name)
    if not new_archive.exists():
        shutil.copy(current_archive, new_archive)

    # voeg de nieuwe zips toe aan de bestaande scenarios
    with zipfile.ZipFile(new_archive, "a") as output_archive:
        for zip_name in lst_zips_nieuwe_export:
            with zipfile.ZipFile(zip_name, "r") as input_archive:
                name_list = input_archive.namelist()
                file_extensions = [name.split(".")[-1] for name in name_list]
                excel_file = name_list[file_extensions.index("xlsx")]
                name_list.remove(excel_file)
                for file in name_list:
                    output_archive.writestr(file, input_archive.open(file).read())

    # verwijder de verwijderde scenarios indien nodig: dit doen we met een tijdelijke zip
    verwijderde_scenario_names = [f"scenario_{id}" for id in verwijderde_scenarios]
    # oude meta data excel moet er uit gehaald worden
    # TODO: meta data bestand kan ook naast de zip worden bewaard: dit zou een stuk sneller zijn
    temp_zip_file = new_archive.with_suffix(".temp.zip")
    with (
        zipfile.ZipFile(new_archive, "r") as input_archive,
        zipfile.ZipFile(temp_zip_file, "w") as output_archive,
    ):
        for item in input_archive.infolist():
            if (
                item.filename.split("/")[0] not in verwijderde_scenario_names
                and item.filename.split(".")[-1] != "xlsx"
            ):
                # verplaats alles naar de nieuwe zip
                output_archive.writestr(item, input_archive.read(item.filename))

    # klaar: verwijder de oude zip
    temp_zip_file.replace(new_archive)

    with zipfile.ZipFile(new_archive, "a") as output_archive:
        # als laaste voeg de meta data weer toe
        output_archive.write(excel_output_dir, arcname=excel_name_in_zip)


if __name__ == "__main__":
    current_dir = Path.cwd()


    # Set up basic logger
    log_file = current_dir / "log_bulk.txt"
    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger()

    # configuration:
    current_archive = current_dir / "merged_nov_2024.zip"
    new_archive = current_dir / "merged_feb_2025_2.zip"

    # zet de LDO api key in de .env file
    if dotenv.load_dotenv():
        environmental_variables = dotenv.dotenv_values()
        LDO_api_key = environmental_variables["LDO_api_key"]
    headers = haal_token_op(LDO_api_key)

    logger.info("haal scenarios op")
    beschikbare_scenario_ids = haal_scenarios_op(
        maximum=10_000, headers=headers
    )  

    logger.info("Vergelijk scenarios")
    verwijderde_scenarios, nieuwe_scenarios, df_current_local_LDO = (
        vergelijke_nieuwe_en_huidige(current_archive, beschikbare_scenario_ids)
    )

    logger.info("Start export scenarios")
    lst_zips_nieuwe_export = export_uit_LDO(
        nieuwe_scenarios=nieuwe_scenarios,
        headers=headers,
    )

    logger.info("Voeg export scenarios samen")
    voeg_zips_samen_verwijder_ouder(
        lst_zips_nieuwe_export,
        verwijderde_scenarios,
        df_current_local_LDO,
        current_archive,
        new_archive,
    )
