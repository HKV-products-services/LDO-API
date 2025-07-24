"""
gemaakt voor LIWO door David Haasnoot (d.haasnoot@hkv.nl)
Jun, 2025
"""

import logging
from pathlib import Path
import dotenv
from update_local_LDO_custom import get_layer_names_from_scenario, haal_scenarios_op, haal_token_op, export_uit_LDO_custom

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


if __name__ == "__main__":
    current_dir = Path.cwd()

    # Set up basic logger
    log_file = current_dir / "log_bulk.txt"
    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger()

    # geef de scenarios op om te exporteren:
    export_scenarios = [345,346]

    # zet de LDO api key in de .env file
    if dotenv.load_dotenv():
        environmental_variables = dotenv.dotenv_values()
        LDO_api_key = environmental_variables["LDO_api_key"]
    headers = haal_token_op(LDO_api_key)

    logger.info("haal scenarios op")
    beschikbare_scenario_ids = haal_scenarios_op(
        maximum=10_000, headers=headers
    )  # misschien later meer dan 10_000?

    logger.info("Vergelijk scenarios")
    overlap_scenarios = list(set(export_scenarios).intersection(beschikbare_scenario_ids))
    niet_gevonden_scenarios = list(set(export_scenarios).difference(beschikbare_scenario_ids))
    if len(niet_gevonden_scenarios) > 0: 
        logger.warning(f'{len(niet_gevonden_scenarios)} scenarios niet gevonden in LDO: {niet_gevonden_scenarios}')

    df_layer_names = get_layer_names_from_scenario( overlap_scenarios, headers=headers,)

    logger.info("Start export scenarios")
    lst_zips_nieuwe_export = export_uit_LDO_custom(
        df_layer_names=df_layer_names,
        work_dir=current_dir,
        headers=headers,
    )