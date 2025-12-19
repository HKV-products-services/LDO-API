# LDO API scripts

Voor het raadplegen van het LDO via de API zijn voor LIWO een aantal scripts ontwikkeld.

- `export_LDO.py` bevat functies voor interactie met de API.

De volgende gebruiken de functies uit `export_LDO.py` en runnen bepaalde functionaliteit:

- `update_local_bulk_LDO.py` download de bulk export en voegt dit toe aan een bestaande export.
- `download_LDO_bulk.py` download de bulk export van specifieke scenarios.
- `download_LDO_ssm_tiffs.py` download tiff bestanden gegenereerd door ssm in het LDO.
- `export_SSM_metadata_uit_LDO_met_API.py` exporteren van ssm metadata.

Alle notebooks zijn identiek, alleen met meer uitleg.

- `delete_or_archive_LDO_bulk.ipynb` verwijderd of archiveert bulk downloads van het LDO portaal.

Voor vragen: `haasnoot@hkv.nl`.

## API key aan maken

Stappen plan voor het aanmaken van een api key.

- Login op [https://ldo.overstromingsinformatie.nl/](https://ldo.overstromingsinformatie.nl/), dit loop via bij12.
- Ga naar [https://ldo.overstromingsinformatie.nl/auth/](https://ldo.overstromingsinformatie.nl/auth/).
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

- Afhankelijk via welke organisatie je toegang hebt, kan het nodig zijn om in de code de `TENANT` variabele aan te passen. Deze kan ook in de `.env` file worden gezet.
  - `TENANT = 0` voor beheerders?
  - `TENANT = 1` voor LIWO
  - `TENANT = 2` voor RWS
  ...

meer informatie staat onderaan of op de [docs](https://ldo.overstromingsinformatie.nl/api/v1/docs)

## License

Copyright (c) 2024 - 2025, HKV lijn in Water

GPL 3.0 or later
