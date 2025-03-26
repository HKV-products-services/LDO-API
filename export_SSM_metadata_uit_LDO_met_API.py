# gemaakt voor LIWO door [David Haasnoot](d.haasnoot@hkv.nl)

import pandas as pd
import dotenv
from update_local_bulk_LDO import haal_scenarios_op, haal_token_op
from export_LDO import get_ssm

# work-around via https://www.overstromingsinformatie.nl/auth/
# 
# onder end-point: https://www.overstromingsinformatie.nl/auth/v1/token 
# vervang body met 
# 
# ```json
# {
#     "tenant": 1
# }
# ```
# 
# Vul die (te lange) refresh token hier onder in, daarmee genereer je de volgende keer weer de access token
# 
# meer info op https://www.overstromingsinformatie.nl/api/v1/docs
# 
# Stop de api key in een `.env` bestand, zie `example.env` als voorbeeld

if __name__ == "__main__":
    # haal de API key op uit de .env file
    if dotenv.load_dotenv():
        environmental_variables = dotenv.dotenv_values()
        LDO_api_key = environmental_variables["LDO_api_key"]\
    # Of zet hier de API key handmatig in
    # LDO_api_key = "abcd"
    headers = haal_token_op(LDO_api_key)

    maximum = 8000
    beschikbare_scenario_ids = haal_scenarios_op(maximum, headers)

    lst_json = [get_ssm(scenario, headers) for scenario in beschikbare_scenario_ids]
    df_metadata = pd.DataFrame(lst_json)
    df_metadata.set_index('scenario_id', inplace=True)
    df_metadata.sort_index(inplace=True)

    all_indexes = df_metadata.index
    filled_scenarios = df_metadata.dropna(subset=['raster_types'])
    df_metadata.to_excel('metadata_ssm.xlsx')
    filled_scenarios.to_excel('metadata_ssm_filled.xlsx')


