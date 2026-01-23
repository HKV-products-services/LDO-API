"""
gemaakt voor LIWO door  David Haasnoot (d.haasnoot@hkv.nl)
Juni, 2025

# DOEL
Bevat functies voor interactie met de API, worden gebruikt door andere scripts.
"""

import warnings
import requests
import json
import time
from datetime import datetime as date
from typing import Optional
from pathlib import Path

"""
Python bestand met helper functies voor het exporteren van scenario's uit de LDO via de API van www.ldo.overstromingsinformatie.nl
Zie `export_SSM_metadata_uit_LDO_met_API.py of update_local_bulk_LOD.py` voor stappen plan voor het aanmaken van een api key.
"""

server = "https://ldo.overstromingsinformatie.nl"


def get_scenario_subset(mode, limit_per_request, offset, headers, extra_filter=""):
    """Get list of scenarios status landelijk gebruik (mode=public)"""
    response = requests.get(
        f"{server}/api/v1/scenarios?mode={mode}&limit={limit_per_request}&offset={offset}&order_by=id{extra_filter}",
        headers=headers,
    )
    if response.status_code == 200:
        result = response.json()
        scenario_list = list(
            (
                item.get("id")
                for item in result["items"]
                if item.get("status") == "quality_checked"
            )
        )
    else:
        warnings.warn(f"{response.status_code}:{response.text}")
        scenario_list = []
    return scenario_list


def get_scenario_list(offset, limit_per_request, maximum, headers, extra_filter=""):
    mode = "public"
    scenario_ids = []

    for offset in range(0, maximum, limit_per_request):
        scenario_ids += get_scenario_subset(
            mode, limit_per_request, offset, headers, extra_filter=extra_filter
        )

    return scenario_ids


def create_new_bulk_export(headers: dict, index: int) -> tuple[str, str, str]:
    """creates new bulk-export with given id"""
    body = json.dumps(
        {
            "name": f"LIWO_export_{date.today().strftime('%Y-%m-%d')}-{index}",
            "type": "bulk_file",
            "description": f"LIWO export {date.today().strftime('%Y-%m-%d')}-{index}",
            "config": {
                "excel_file_name": f"LIWO_{date.today().strftime('%Y-%m-%d')}-{index}.xlsx"
            },
        }
    )

    response = requests.post(
        f"{server}/api/v1/bulk-exports", headers=headers, data=body
    )

    if response.status_code == 201:
        export_id = response.json()["id"]
        export_name = response.json()["name"]
        export_description = response.json()["description"]
        # print(f"ID van nieuwe export {export_id}")
    else:
        # print(response.status_code, response.text)
        raise UserWarning(f"{response.status_code}: {response.text}")

    return export_name, export_description, export_id


def delete_bulk_export(headers: dict, index: int) -> tuple[str, str, str]:
    """delete given bulk-export"""

    response = requests.delete(f"{server}/api/v1/bulk-exports/{index}", headers=headers)

    if int(response.status_code) == 204:
        success = True
    elif int(response.status_code) == 404:
        success = False
        warnings.warn(
            f"Bulk export with id {index} not found, it may have already been deleted.",
            category=RuntimeWarning,
        )
    else:
        success = False
        warnings.warn(f"{response.status_code}: {response.text}", category=UserWarning)

    return success, response


def delete_bulk_export_errors(headers: dict, index: int) -> tuple[str, str, str]:
    """delete errors of given bulk-export"""

    response = requests.delete(
        f"{server}/api/v1/bulk-exports/{index}/errors", headers=headers
    )

    if response.status_code == 204:
        success = True
    elif response.status_code == 404:
        success = False
        warnings.warn(
            f"Bulk export with id {index} not found, it may have already been deleted.",
            category=RuntimeWarning,
        )
    else:
        success = False
        warnings.warn(f"{response.status_code}: {response.text}", category=UserWarning)

    return success, response


def archive_bulk_export(headers: dict, index: int) -> tuple[str, str, str]:
    """archvie given bulk-export"""

    body = json.dumps({"status": "archived"})
    response = requests.patch(
        f"{server}/api/v1/bulk-exports/{index}", headers=headers, data=body
    )

    if int(response.status_code) == 200:
        success = True
    elif int(response.status_code) == 400:
        success = False
        warnings.warn(
            f"Bulk export with id {index} not found, it may have already been deleted.",
            category=RuntimeWarning,
        )
    else:
        success = False
        warnings.warn(f"{response.status_code}: {response.text}", category=UserWarning)

    return success, response


def list_bulk_export(headers: dict) -> tuple[str, str, str]:
    """lists all the bulk-export"""

    response = requests.get(f"{server}/api/v1/bulk-exports", headers=headers)

    if response.status_code == 200:
        data = response.json()["items"]
        total, limit = response.json()["total"], response.json()["limit"]
        for i in range(limit, total + limit, limit):
            response = requests.get(
                f"{server}/api/v1/bulk-exports?limit={limit}&offset={i}",
                headers=headers,
            )
            if response.status_code == 200:
                data.extend(response.json()["items"])
            else:
                raise UserWarning(f"{response.status_code}: {response.text}")

        pass
    else:
        # print(response.status_code, response.text)
        raise UserWarning(f"{response.status_code}: {response.text}")

    return data, response


def check_export_id(export_id: str, headers: dict) -> requests.Response:
    """Get bulk export by id"""
    response = requests.get(
        f"{server}/api/v1/bulk-exports/{export_id}", headers=headers
    )

    if response.status_code == 200:
        # print(f"{response.text}")
        pass
    else:
        raise UserWarning(f"{response.status_code}: {response.text}")
    return response


def add_ids_to_export(
    scenario_ids: list[int],
    headers: dict,
    export_name: str,
    export_description: str,
    export_id: str,
) -> requests.Response:
    """Update a bulk exports with scenario_ids"""

    body = json.dumps(
        {
            "name": export_name,
            "description": export_description,
            "scenario_ids": scenario_ids,
        }
    )

    response = requests.patch(
        f"{server}/api/v1/bulk-exports/{export_id}", headers=headers, data=body
    )

    if response.status_code == 200:
        # print(f"{response.json()}")
        # print(len(response.json()['scenario_ids']))
        pass
    else:
        # print(response.status_code, response.text)
        raise UserWarning(f"{response.status_code}: {response.text}")

    return response


def start_export(
    headers: dict, export_name: str, export_description: str, export_id: str
) -> tuple[requests.Response, dict]:
    """Start bulk export"""
    export_body = json.dumps(
        {"name": export_name, "description": export_description, "status": "submitted"}
    )

    response = requests.patch(
        f"{server}/api/v1/bulk-exports/{export_id}", headers=headers, data=export_body
    )

    if response.status_code == 200:
        response = requests.get(
            f"{server}/api/v1/bulk-exports/{export_id}", headers=headers
        )
        if response.status_code == 200:
            status = response.json()["status"]
            status
            # print(f"status van export met id {export_id} is {status}")
        else:
            warnings.warn(f"{response.status_code}: {response.text}")

    else:
        warnings.warn(f"{response.status_code}: {response.text}")

    return response, export_body


def wait_for_export(
    export_id: str, headers: dict, status: str
) -> Optional[requests.Response]:
    """wacht tot export klaar is voor download"""
    while status == "submitted":
        response = requests.get(
            f"{server}/api/v1/bulk-exports/{export_id}", headers=headers
        )
        if response.status_code == 200:
            status = response.json()["status"]
            # print(f"status taak: {status}", end="\r")
            if status == "error":
                response = None
                break
        else:
            warnings.warn(f"{response.status_code}: {response.text}")
        time.sleep(10)

    return response


def get_file_name(export_id: str, headers: dict, export_body: dict) -> str:
    """haal de bestandsnaam van de export op"""
    response = requests.get(
        f"{server}/api/v1/bulk-exports/{export_id}", headers=headers, data=export_body
    )

    if response.status_code == 200:
        file_name = response.json()["files"]
        # print(f"{file_name}")
    else:
        warnings.warn(f"{response.status_code}: {response.text}")

    return file_name


def get_download_url(
    server: str, export_id: str, headers: dict, export_body: dict
) -> str:
    """haal de download URL op van de export"""
    response = requests.get(
        f"{server}/api/v1/bulk-exports/{export_id}/files/export_{export_id}.zip/download",
        headers=headers,
        data=export_body,
    )

    if response.status_code == 200:
        url = response.json()["url"]
        # print(f"{url}")
    else:
        warnings.warn(f"{response.status_code}: {response.text}")
        url = None

    return url


def download_zip(url: str, export_id: str) -> None:
    """download het zip bestand"""
    response = requests.get(url, stream=True)
    with open(f"export_{export_id}.zip", "wb") as f:
        for chunk in response.iter_content(chunk_size=512):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)


def get_file_url(scenario_id: str, layer_name: str, headers: dict) -> str:
    """haal de bestanden van de export op

    Specifiek voor "Mortality.tif","Total_damage.tif","Total_victims.tif","Total_affected.tif"
    """
    response = requests.get(
        f"{server}/api/v1/scenarios/{scenario_id}/files/{layer_name}/download",
        headers=headers,
    )

    if response.status_code == 200:
        url = response.json()["url"]
        return response.status_code, url
    else:
        return response.status_code, response.text


def download_tif(url: str, name: str, export_id: str, work_dir: Path) -> None:
    """download een tif bestand"""
    response = requests.get(url, stream=True)

    export_path = work_dir / f"{export_id}"
    if not export_path.exists():
        export_path.mkdir()
    if name.startswith("scenario_"):
        new_name = "_".join(name.split("_")[2:])
    else:
        new_name = name
    fname = export_path / new_name

    with open(fname, "wb") as f:
        for chunk in response.iter_content(chunk_size=512):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)


def get_ssm(scenario_id: str, headers: dict) -> str:
    """haal de bestandsnaam van de export op"""
    response = requests.get(
        f"{server}/api/v1/scenarios/{scenario_id}/external-processings", headers=headers
    )

    if response.status_code == 200:
        json = response.json()["items"][0]
        if json["config"] is not None:
            json.update(json["config"])
            del json["config"]
        if json["meta_data"] is not None:
            json.update(json["meta_data"])
            del json["meta_data"]
        del json["id"], json["status"]
        if "raster_types" in json:
            json["raster_types"] = ";".join(json["raster_types"])
        if len(json["errors"]) == 0:
            json["errors"] = None
        else:
            json["errors"] = ";".join(json["errors"])

        return json
    else:
        warnings.warn(f"{response.status_code}: {response.text}")
        return None


def combine_functions_start_export(
    headers: dict, index: int, scenario_ids: list
) -> tuple[str, str, dict]:
    """maakt een nieuwe (lege) bulk-export aan, voegt ids toe en start de export"""
    export_name, export_description, export_id = create_new_bulk_export(headers, index)
    response = check_export_id(export_id, headers)
    response = add_ids_to_export(
        scenario_ids, headers, export_name, export_description, export_id
    )
    response, export_body = start_export(
        headers, export_name, export_description, export_id
    )
    status = response.json()["status"]
    return export_id, status, export_body


def combine_functions_download_export(
    headers: dict, export_id: int, status, export_body: dict
) -> None:
    """Wacht tot de gegenereerde export is voltooid en download deze vervolgens"""
    response = wait_for_export(export_id, headers, status)
    if response is not None:
        url = get_download_url(server, export_id, headers, export_body)
        download_zip(url, export_id)
    else:
        warnings.warn(
            f"Export with id {export_id} had an issue downloading, moving onto the next"
        )


def status_update(export_id: str, headers: dict) -> str:
    """haal de status van de export op"""

    body = {"status": "quality_checked"}
    response = requests.patch(
        f"{server}/api/v1/scenarios/{export_id}/status",
        headers=headers,
        body=body,
    )

    if response.status_code == 200:
        status = response.json()["status"]
        return status
    else:
        warnings.warn(f"{response.status_code}: {response.text}")
        return None


def get_layer_names(export_id: str, headers: dict) -> str:
    """haal de bestandsnaam van een scenario op om vervolgens te exporteren"""
    response = requests.get(f"{server}/api/v1/scenarios/{export_id}", headers=headers)

    if response.status_code == 200:
        names = response.json()["files"].keys()
        return list(names)
    else:
        warnings.warn(f"{response.status_code}: {response.text}")
        return None


def get_all_metadata(scenario_ids: list, fname: Path, headers: dict) -> str:
    """haal de bestanden van de export op"""
    data = dict()
    data["id"] = scenario_ids
    response = requests.post(
        f"{server}/api/v1/scenarios/export",
        data=json.dumps(data).replace(" ", ""),
        headers=headers,
        stream=True,
    )

    if response.status_code == 200:
        with open(fname, "wb") as f:
            for chunk in response.iter_content(chunk_size=512):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)

    else:
        return response.status_code, response.text
