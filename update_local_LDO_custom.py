"""
gemaakt voor LIWO door David Haasnoot (d.haasnoot@hkv.nl)
Jun, 2025


# DOEL
Download de bulk export en voegt dit toe aan een bestaande export.

"""

import logging
from pathlib import Path
import zipfile
from export_LDO import download_tif, get_all_metadata, get_scenario_list, get_layer_names, get_file_url
import pandas as pd
import requests
import shutil
from tqdm import tqdm

"""
Stappen plan voor het aanmaken van een api key.
- Login op https://ldo.overstromingsinformatie.nl/, dit loop via bij12.
- Ga naar https://ldo.overstromingsinformatie.nl/auth/
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
- Bewaar die hele `'key'` in een bestand die `.env` heet, zie `.env.example` voor het formaat.
meer informatie staat onderaan of op de docs: https://ldo.overstromingsinformatie.nl/api/v1/docs
- Afhankelijk via welke organisatie je toegang hebt, kan het nodig zijn om in de code de `TENANT` variabele
    aan te passen. Deze kan ook in de `.env` file worden gezet.
  - `TENANT = 0` voor beheerders? 
  - `TENANT = 1` voor LIWO
  - `TENANT = 2` voor RWS
  ...
"""

current_dir = Path(__file__).parent

# Set up basic logger
log_file = current_dir / "log_bulk.txt"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger()


def haal_token_op(api_key: str, tenant:int) -> dict:
    """Haal de access token op voor www.ldo.overstromingsinformatie.nl gegeven de api key"""
    url_auth = "https://ldo.overstromingsinformatie.nl/auth/v1/token/"
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    response = requests.post(
        url=url_auth, headers=headers, json={"tenant": tenant}, auth=("__key__", api_key)
    )
    try:
        id_token = response.json()["access"]
    except KeyError:
        res_error = response.text
        if '"tenant"' in res_error and 'Invalid pk' in res_error and 'object does not exist.' in res_error: 
            tenant = res_error.removeprefix('{"tenant":["Invalid pk \\"')[0]
            logger.error(f"Invalid tenant {tenant} for the provided API key.")
            raise UserWarning(f"Invalid tenant {tenant} for the provided API key, adjust the TENANT variable accordingly.")
        else:
            logger.error(f"Failed to retrieve access token: {res_error}")
            raise UserWarning(f"Failed to retrieve access token: {res_error}")
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {id_token}",
    }
    return headers


def haal_scenarios_op(maximum: int, headers: dict, extra_filter: str = "") -> list:
    """ "Haal de scenario ids op"""
    limit_per_request = 100
    offset = 0
    beschikbare_scenario_ids = get_scenario_list(
        offset, limit_per_request, maximum, headers, extra_filter=extra_filter
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


def get_layer_names_from_scenario(nieuwe_scenarios: str, headers: dict) -> pd.DataFrame:
    """Haal de bestandsnamen van de scenarios op om vervolgens te exporteren"""
    data = {ids: get_layer_names(ids, headers) for ids in nieuwe_scenarios}
    max_length = max([len(data[ids]) for ids in data.keys()])
    # Fill each names list to max_length with None
    data = {ids: list(name) + [None] * (max_length - len(name)) for ids, name in data.items()}
    return pd.DataFrame(data).T



def export_uit_LDO_custom(
    df_layer_names: pd.DataFrame, work_dir: Path, headers: dict
) -> None:
    export_dir = work_dir / "downloaded_tiffs"
    export_dir.mkdir(exist_ok=True)
    missing_values = {}
    error = None
    try:
        for scenario_id, row in tqdm(df_layer_names.iterrows()):
            for file_name in row.tolist():
                # valid_name = validate_file_name(file_name)
                if file_name is None or file_name == "":
                    continue
                # Check if the file name is valid
                status_code, url = get_file_url(scenario_id, file_name, headers)
                if status_code == 200:
                    try:
                        download_tif(url, file_name, scenario_id, export_dir)
                    except ConnectionError as e:
                        logger.error(f"Connection error during download: {e}")
                        # try again
                        try:
                            download_tif(url, file_name, scenario_id, export_dir)
                        except ConnectionError as e:
                            logger.error(
                                f"Connection error during download (2nd try): {e}, {url}, {scenario_id}"
                            )
                            continue

                else:
                    logger.info(
                        f"Failed to download {file_name} for scenario {scenario_id}: {url}"
                    )
                    if scenario_id in missing_values:
                        missing_values[scenario_id].append(file_name)
                    else:
                        missing_values[scenario_id] = [file_name]

        error = get_all_metadata(
            scenario_ids=df_layer_names.index.tolist(),
            fname=export_dir / "metadata.xlsx",
            headers=headers,
        )
    except Exception as e:
        logger.error(f"Error during download: {e} {error}")

    # still write the missing values to a csv & zip
    missing_values_df = pd.DataFrame.from_dict(missing_values, orient="index")
    missing_values_df.to_csv(export_dir / "missing_values.csv")
    with zipfile.ZipFile(work_dir / "downloaded_tiffs.zip", "w") as zipf:
        for folder in export_dir.iterdir():
            zipf.write(folder, folder.name)
            for file in folder.iterdir():
                zipf.write(file, folder / file.name)




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
