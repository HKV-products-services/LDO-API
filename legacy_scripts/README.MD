# Legacy LDO API scripts

De legacy alternatieven voor het downloaden zijn:

- `update_local_bulk_LDO.py` download de bulk export en voegt dit toe aan een bestaande export.
- `download_LDO_bulk.py` download de bulk export van specifieke scenarios, (niet aanbevolen, deze gebruikt de functies van het LDO die traag zijn)
- `download_LDO_ssm_tiffs.py` download tiff bestanden gegenereerd door ssm in het LDO.

Overige scripts:

- `delete_or_archive_LDO_bulk.ipynb` -> hulp scripts voor verwijderen van LDO scenarios.
- `filter_scenarios.ipynb` -> bepalen van subset gegeven locaties

Deze worden minder vaak getest. Indien je de package niet instaleerd maar alleen cloned, kan moet je deze scripts naar de hoofd map verplaatsen om zet te gebruiken, anders kan python de `LDO_API` package niet vinden.
