"""
gemaakt voor LIWO door David Haasnoot (d.haasnoot@hkv.nl)
Jun, 2025
"""

import pandas as pd
import dotenv
from LDO_API.update_local_LDO_custom import haal_scenarios_op, haal_token_op
from LDO_API import get_ssm

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
- scrol naar beneden (maar nog in het zelfde groene vak), in de onderste 2 zwarte vlakken zie je response code 201 
    als het gelukt is.
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
- Afhankelijk via welke organisatie je toegang hebt, kan het nodig zijn om in de code de `TENANT` variabele aan 
    te passen. Deze kan ook in de `.env` file worden gezet.
  - `TENANT = 0` voor beheerders? 
  - `TENANT = 1` voor LIWO
  - `TENANT = 2` voor RWS
  ...
"""
if __name__ == "__main__":
    # haal de API key op uit de .env file
    if dotenv.load_dotenv():
        environmental_variables = dotenv.dotenv_values()
        LDO_api_key = environmental_variables["LDO_api_key"]
    # Of zet hier de API key handmatig in
    # LDO_api_key = "abcd"
    TENANT : int  = 1 # 0, 1, 2 ...
    headers = haal_token_op(LDO_api_key, tenant=TENANT)

    maximum = 10_000  
    beschikbare_scenario_ids = haal_scenarios_op(maximum, headers)

    lst_json = [get_ssm(scenario, headers) for scenario in beschikbare_scenario_ids]
    df_metadata = pd.DataFrame(lst_json)
    df_metadata.set_index('scenario_id', inplace=True)
    df_metadata.sort_index(inplace=True)

    all_indexes = df_metadata.index
    filled_scenarios = df_metadata.dropna(subset=['raster_types'])
    df_metadata.to_excel('metadata_ssm.xlsx')
    filled_scenarios.to_excel('metadata_ssm_filled.xlsx')


