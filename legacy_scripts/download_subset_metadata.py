"""
gemaakt voor LIWO door David Haasnoot (d.haasnoot@hkv.nl)
Jun, 2025
"""

import logging
from pathlib import Path
import dotenv
from LDO_API.export_LDO import get_all_metadata
from LDO_API.update_local_LDO_custom import (
    get_layer_names_from_scenario,
    haal_scenarios_op,
    haal_token_op,
)

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


def main():
    current_dir = Path.cwd()

    # Set up basic logger
    log_file = current_dir / "log_bulk.txt"
    logging.basicConfig(
        filename=log_file,
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(name="LDO-API-download")

    # geef de scenarios op om te exporteren:

    # zet de LDO api key in de .env file
    if dotenv.load_dotenv():
        environmental_variables = dotenv.dotenv_values()
        LDO_api_key = environmental_variables["LDO_api_key"]
        TENANT = int(environmental_variables.get("TENANT", 1))

    headers = haal_token_op(LDO_api_key, tenant=TENANT)
    logger.info("haal scenarios op")
    beschikbare_scenario_ids = haal_scenarios_op(
        maximum=12_000, headers=headers
    )  # misschien later meer dan 10_000?

    get_metadata(
        logger,
        beschikbare_scenario_ids,
        current_dir,
        headers,
    )


def get_metadata(logger, beschikbare_scenario_ids, current_dir, headers):
    logger.info(f"totaal {len(beschikbare_scenario_ids)} ids")

    download_dir = current_dir / "subset_tiffs"

    export_dir = download_dir 
    downloaded_scenarios = list(
        [int(f.name) for f in download_dir.glob("*") if f.is_dir()]
    )
    logger.info(f"totaal {len(downloaded_scenarios)} downloaded ids")
    df_layer_names = get_layer_names_from_scenario(
        downloaded_scenarios,
        headers=headers,
    )

    df_layer_names.to_csv(export_dir / "layer_names_per_scenario.csv", index=False)

    logger.info("Start export scenarios")
    get_all_metadata(
        scenario_ids=df_layer_names.index.tolist(),
        fname=export_dir / "metadata.xlsx",
        headers=headers,
    )


if __name__ == "__main__":
    main()
