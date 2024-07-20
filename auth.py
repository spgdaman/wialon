# import requests
# import json

# # r = requests.get("https://hosting.wialon.com/login.html?client_id=wialon&access_type=256&activation_time=0&duration=0&lang=en&flags=0&user=Kenpoly", auth=('Kenpoly', 'Kenpoly@24'))
# r = requests.get('https://hst-api.wialon.com/wialon/ajax.html?svc=token/login&params={"token":"98af207443191952de3c0d6c7b1d1e7bEEA2F8DD25ADCA95BD68D7C83D6BE334B587D98E"}')
# r = str(r.text)
# response = json.loads(r)
# sid = response['eid']
# print(json.dumps(response, indent=1))
# print(type(r))

# r = requests.get('https://hst-api.wialon.com/wialon/ajax.html?svc=core/search_items&params={"spec":{"itemsType":"avl_unit","propName":"sys_name","propValueMask":"*","sortType":"sys_name"},"force":1,"flags":1,"from":0,"to":0}&sid=sid')
# r = str(r.text)
# response = json.loads(r)
# print(json.dumps(response, indent=1))

# r = requests.get('https://hst-api.wialon.com/wialon/ajax.html?svc=report/exec_report&params={"reportResourceId":1,"reportTemplateId":1,"reportObjectId":1,"reportObjectSecId":0,"interval":{"from":1720608708,"to":1720695108,"flags":0}}&sid=sid')
# r = str(r.text)
# response = json.loads(r)
# print(json.dumps(response, indent=1))

import requests
import json
from collections import deque
import pandas as pd

def remove_keys(d, keys_to_remove):
    # Queue to store dictionaries to be processed
    # queue = deque([(1, d)])  # Starting with the main dictionary and a key 1
    # result = {}

    # while queue:
    #     current_key, current_dict = queue.popleft()
    #     if isinstance(current_dict, dict):
    #         # Remove specified keys from the current dictionary
    #         new_dict = {k: v for k, v in current_dict.items() if k not in keys_to_remove}
    #         result[current_key] = new_dict
    #         for k, v in new_dict.items():
    #             if isinstance(v, dict):
    #                 # Add nested dictionaries to the queue with an incremental key
    #                 queue.append((len(result) + 1, v))
    #             elif isinstance(v, list):
    #                 for item in v:
    #                     if isinstance(item, dict):
    #                         queue.append((len(result) + 1, item))
    
    # return result
    if isinstance(d, dict):
        # Remove each key if it exists in the current dictionary level
        for key in keys_to_remove:
            if key in d:
                del d[key]
        # Recursively call this function for each value in the dictionary
        for key in list(d.keys()):  # Use list(d.keys()) to avoid 'dictionary changed size during iteration' error
            remove_keys(d[key], keys_to_remove)
    elif isinstance(d, list):
        # If the dictionary contains lists, recursively call this function for each item in the list
        for item in d:
            remove_keys(item, keys_to_remove)

def rename_keys(d, key_map):
    new_data = {}
    for old_key, value in d.items():
        new_value = {}
        for inner_key, inner_value in value.items():
            new_key = key_map.get(inner_key, inner_key)
            new_value[new_key] = inner_value
        new_data[old_key] = new_value
    return new_data

# Define your API token and base URL
API_TOKEN = '98af207443191952de3c0d6c7b1d1e7b5300B4E05C4637A09E764A3A2D2E61F54A1745BE'
BASE_URL = 'https://hst-api.wialon.com/wialon/ajax.html?'

# Function to authenticate and get a session ID
def authenticate():
    url = f"{BASE_URL}svc=token/login&params={{\"token\":\"{API_TOKEN}\"}}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if 'error' not in data:
            return data['eid']  # Extract the session ID from the response
        else:
            raise Exception(f"Error in authentication: {data['error']}")
    else:
        raise Exception(f"Failed to connect to server: {response.status_code}")

session_id = authenticate()

def cleanup_result():
    session_id = authenticate()
    url = "https://hst-api.wialon.com/wialon/ajax.html?svc=report/cleanup_result&params={}&sid={session_id}"
    response = requests.get(url)
    print(response.json())
    return response.json()

# Function to get data
def search_items():
    session_id = authenticate()
    # params = {"spec":{"itemsType":"avl_resource","propName":"","propValueMask":"","sortType":"","propType":"","or_logic":False},"force":1,"flags":8193,"from":0,"to":0}
    params = {"spec":{"itemsType":"avl_unit_group","propName":"","propValueMask":"","sortType":"","propType":"","or_logic":False},"force":1,"flags":1,"from":0,"to":0}
    # params = {
    #             "spec": {
    #                 "itemsType": "avl_unit",
    #                 "propName": "sys_name",
    #                 "propValueMask": "*",
    #                 "sortType": "sys_name"
    #             },
    #             "force": 1,
    #             "flags": 1,
    #             "from": 0,
    #             "to": 0
    #         }
    url = f"{BASE_URL}svc=core/search_items&sid={session_id}&params={json.dumps(params)}"
    response = requests.get(url)
    if response.status_code == 200:
        # return response.json()
        print(response.json())
        return response.json()
    else:
        raise Exception(f"Failed to retrieve data: {response.status_code}")
    
def group_unit_items():
    # session_id = session_id
    params = {"spec":{"itemsType":"avl_unit_group","propName":"","propValueMask":"","sortType":"","propType":"","or_logic":False},"force":1,"flags":1,"from":0,"to":0}
    url = f"{BASE_URL}svc=core/search_items&sid={session_id}&params={json.dumps(params)}"
    response = requests.get(url)
    if response.status_code == 200:
        # return response.json()
        response = dict(response.json())
        response = response.get("items")[1:]
        keys_to_remove = ["cls", "mu", "uacl"]
        
        remove_keys(response, keys_to_remove)
        response = {i: d for i, d in enumerate(response, start=1)}

        key_map = {"nm": "unit_group", "id": "unit_group_id", "u": "units"}
        return rename_keys(response, key_map)
    else:
        raise Exception(f"Failed to retrieve data: {response.status_code}")

def search_unit_type():
   session_id = authenticate()
   params = {
            "spec": {
                "itemsType": "avl_unit",
                "propName": "rel_hw_type_name,rel_last_msg_date",
                "propValueMask": "*",
                "sortType": "rel_creation_time"
            },
            "force": 1,
            "flags": 1,
            "from": 0,
            "to": 0
        } 
   url = f"{BASE_URL}svc=core/search_items&sid={session_id}&sid={session_id}&params={json.dumps(params)}"
   response = requests.get(url)
   if response.status_code == 200:
        print(response.json())
        return response.json()
   else:
        raise Exception(f"Failed to retrieve data: {response.status_code}")
   
def search_unit_groups():
   session_id = authenticate()
   params = {
            "spec": {
                "itemsType": "avl_unit_group",
                "propName": "rel_user_creator_name,rel_group_unit_count",
                "propValueMask": "*",
                "sortType": "sys_name"
            },
            "force": 1,
            "flags": 133,
            "from": 0,
            "to": 0
        } 
   url = f"{BASE_URL}svc=core/search_items&sid={session_id}&sid={session_id}&params={json.dumps(params)}"
   response = requests.get(url)
   if response.status_code == 200:
        print(response.json())
        return response.json()
        # return response.json()
   else:
        raise Exception(f"Failed to retrieve data: {response.status_code}")
   
def exec_report():
    session_id = authenticate()
    params={
		"reportResourceId":27916207,
		"reportTemplateId":2,
		# "reportObjectId":27916303,
        "reportObjectId":27922767,
		"reportObjectSecId":0,
		"interval":{
			"from": 1717243200,
			"to":1721304000,
			"flags":0
		}
	}
    url = f"{BASE_URL}svc=report/exec_report&sid={session_id}&sid={session_id}&params={json.dumps(params)}"
    response = requests.get(url)
    # if response.status_code == 200:
    #     print(response.json())
    #     response = response.json()
    #     df = pd.json_normalize(response)
    #     # print(df)
    #     df.to_csv("exec_report.csv", index=False)
    #     # return response.json()
    # else:
    #     raise Exception(f"Failed to retrieve data: {response.status_code}")

    params={
        "tableIndex": 0,
        "indexFrom": 0,
        "indexTo": 0
    }
    url = f"{BASE_URL}svc=report/get_result_rows&sid={session_id}&params={json.dumps(params)}"
    response = requests.get(url)
    print(response.json())
    return response.json()
    
def report_tables():
    session_id = authenticate()
    params={

    }
    url = f"{BASE_URL}svc=report/get_report_tables&sid={session_id}&sid={session_id}&params={json.dumps(params)}"
    response = requests.get(url)
    if response.status_code == 200:
        response = response.json()
        df = pd.json_normalize(response)
        # print(df)
        df.to_csv("report_tables.csv", index=False)
        return df.to_json()
    else:
        raise Exception(f"Failed to retrieve data: {response.status_code}")
    
def report_data():
    session_id = authenticate()
    params={
        "itemId": 27916207,
        "col": [1,2,3,4,5,6,7,8,9,10,11,12,13,14],
        "flags": 0
    }
    url = f"{BASE_URL}svc=report/get_report_data&sid={session_id}&sid={session_id}&params={json.dumps(params)}"
    response = requests.get(url)
    if response.status_code == 200:
        print(response.json())
        response = response.json()
        df = pd.json_normalize(response)
        # print(df)
        df.to_csv("report_data.csv", index=False)
        return df.to_json()
    else:
        raise Exception(f"Failed to retrieve data: {response.status_code}")
    
def result_rows():
    session_id = authenticate()
    params={
        "tableIndex": 0,
        "indexFrom": 0,
        "indexTo": 0
    }
    url = f"{BASE_URL}svc=report/get_result_rows&sid={session_id}&params={json.dumps(params)}"
    response = requests.get(url)
    print(response.json())
    return response.json()

def list_of_units(data):
    unit_groups = []
    unit_group_ids = []
    units = []

    # Iterate over the dictionary and unpack each entry
    for key, value in data.items():
        unit_group = value['unit_group']
        unit_group_id = value['unit_group_id']
        for unit in value['units']:
            unit_groups.append(unit_group)
            unit_group_ids.append(unit_group_id)
            units.append(unit)

    # Create a DataFrame
    df = pd.DataFrame({
        'Unit_Group': unit_groups,
        'Unit_Group_ID': unit_group_ids,
        'Unit': units
    })
    return df

def summary_report(time_from, time_to):
    """
    time_from and time_to are unix timestamps for the start and end date
    """

    data = group_unit_items()
    data = list_of_units(data)
    data = data["Unit"].tolist()
    print(data)
    print(len(data))

    result = []

    for d in data:
        # Report execution
        params={
            "reportResourceId":27916207,
            "reportTemplateId":2,
            "reportObjectId":d,
            "reportObjectSecId":0,
            "interval":{
                "from": time_from,
                "to":time_to,
                "flags":0
            }
        }
        url = f"{BASE_URL}svc=report/exec_report&sid={session_id}&sid={session_id}&params={json.dumps(params)}"
        response = requests.get(url)

        # fetching report data
        params={
            "tableIndex": 0,
            "indexFrom": 0,
            "indexTo": 0
        }
        url = f"{BASE_URL}svc=report/get_result_rows&sid={session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        result.append(response.json())
        print(response.json())
        print(len(result))
    print(result)

    cleaned_data = []
    for entry in result:
        vehicle_info = {
            'Grouping': entry[0]['c'][0],
            'Km/l': entry[0]['c'][1],
            'Mileage': entry[0]['c'][2],
            'Max_Speed': entry[0]['c'][3]['t'],
            'Latitude': entry[0]['c'][3]['y'],
            'Longitude': entry[0]['c'][3]['x'],
            'Unit': entry[0]['c'][3]['u'],
            'Engine_Hours': entry[0]['c'][4],
            'Fuel_Consumption': entry[0]['c'][5],
            'Initial_Fuel_Level': entry[0]['c'][6],
            'Final_Fuel_Level': entry[0]['c'][7],
            'Total_Fillings': entry[0]['c'][8],
            'Total_Drains': entry[0]['c'][9],
            'Fuel_Filled': entry[0]['c'][10],
            'Fuel_Drained': entry[0]['c'][11]
        }
        cleaned_data.append(vehicle_info)
    print(pd.DataFrame(cleaned_data))
    json_str = pd.DataFrame(cleaned_data).to_json()
    return json_str

def trips(time_from, time_to):
    """
    time_from and time_to are unix timestamps for the start and end date
    """

    data = group_unit_items()
    data = list_of_units(data)
    data = data["Unit"].tolist()
    print(data)
    print(len(data))

    result = []

    for d in data:
        # Report execution
        params={
            "reportResourceId":27916207,
            "reportTemplateId":1,
            "reportObjectId":d,
            "reportObjectSecId":0,
            "interval":{
                "from": time_from,
                "to":time_to,
                "flags":0
            }
        }
        url = f"{BASE_URL}svc=report/exec_report&sid={session_id}&sid={session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        
        # fetching report data
        params={
            "tableIndex": 0,
            "rowIndex": 0
        }
        url = f"{BASE_URL}svc=report/get_result_subrows&sid={session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        result.append(response.json())
        print(response.json())
        print(len(result))
    print(result)

    file_path = 'vehicle_data.json'
    with open(file_path, 'w') as json_file:
        json.dump(result, json_file, indent=4)

    print(f"JSON data has been successfully written to {file_path}")

    cleaned_data = []
    for entry in result:
        vehicle_info = {
            'Grouping': entry[0]['c'][0],
            'Beginning_Date_Time': entry[0]['c'][1]['t'],
            'Initial_Location': entry[0]['c'][2]['t'],
            'End_Date_Time': entry[0]['c'][3]['t'],
            'Final_Location': entry[0]['c'][4]['t'],
            'Duration': entry[0]['c'][5],
            'Mileage': entry[0]['c'][6],
            'Avg Speed': entry[0]['c'][7]
        }
        cleaned_data.append(vehicle_info)
    print(pd.DataFrame(cleaned_data))
    json_str = pd.DataFrame(cleaned_data).to_json()
    return json_str

def refueling_and_drops(time_from, time_to):
    """
    time_from and time_to are unix timestamps for the start and end date
    """

    data = group_unit_items()
    data = list_of_units(data)
    data = data["Unit"].tolist()
    print(data)
    print(len(data))

    result = []

    for d in data:
        # Report execution
        params={
            "reportResourceId":27916207,
            "reportTemplateId":3,
            "reportObjectId":d,
            "reportObjectSecId":0,
            "interval":{
                "from": time_from,
                "to":time_to,
                "flags":0
            }
        }
        url = f"{BASE_URL}svc=report/exec_report&sid={session_id}&sid={session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        
        # fetching report data
        params={
            "tableIndex": 0,
            "rowIndex": 0
        }
        url = f"{BASE_URL}svc=report/get_result_subrows&sid={session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        result.append(response.json())
        print(response.json())
        print(len(result))
    print(result)

    file_path = 'vehicle_data.json'
    with open(file_path, 'w') as json_file:
        json.dump(result, json_file, indent=4)

    print(f"JSON data has been successfully written to {file_path}")

    cleaned_data = []
    for entry in result:
        for e in entry:
            vehicle_info = {
                'Grouping': "" if e['c'][0] is None else e['c'][0],
                'Time': e['c'][1]['t'] if isinstance(e['c'][1], dict) else "",
                'Location': e['c'][2]['t'] if isinstance(e['c'][2], dict) else "",
                'Initial_Fuel_Level':  e['c'][3] if e['c'][3] != "-----" else "",
                'Filled':  e['c'][4] if e['c'][4] != "-----" else "",
                'Final_Fuel_Level':  e['c'][5] if e['c'][5] != "-----" else "",
                'Sensor_Name':  e['c'][6]['t'] if isinstance(e['c'][6], dict) else ""
            }
            cleaned_data.append(vehicle_info)
    print(pd.DataFrame(cleaned_data))
    data = pd.DataFrame(cleaned_data)
    data.to_csv('refuel_and_drops.csv', index=False)
    print(f"DataFrame has been successfully written to refuel_and_drops.csv")
    return data.to_json()

def geofence(time_from, time_to):
    """
    time_from and time_to are unix timestamps for the start and end date
    """

    data = group_unit_items()
    data = list_of_units(data)
    data = data["Unit"].tolist()
    print(data)
    print(len(data))

    result = []

    for d in data:
        # Report execution
        params={
            "reportResourceId":27916207,
            "reportTemplateId":6,
            "reportObjectId":d,
            "reportObjectSecId":0,
            "interval":{
                "from": time_from,
                "to":time_to,
                "flags":0
            }
        }
        url = f"{BASE_URL}svc=report/exec_report&sid={session_id}&sid={session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        
        # fetching report data
        params={
            "tableIndex": 0,
            "rowIndex": 0
        }
        url = f"{BASE_URL}svc=report/get_result_subrows&sid={session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        result.append(response.json())
        print(response.json())
        print(len(result))
    print(result)

    file_path = 'geofence_report.json'
    with open(file_path, 'w') as json_file:
        json.dump(result, json_file, indent=4)

    print(f"JSON data has been successfully written to {file_path}")

    cleaned_data = []
    for entry in result:
        for e in entry:
            vehicle_info = {
                'Grouping': e['c'][0],
                'Geofence': e['c'][1],
                'Time_In': e['c'][2]['t'],
                'Time_Out': e['c'][3]['t'],
                'Duration_In': e['c'][4],
                'Total_Time': e['c'][5],
                'Driver': e['c'][6]

            }
            cleaned_data.append(vehicle_info)
    print(vehicle_info)
    print(pd.DataFrame(cleaned_data))

    json_str = pd.DataFrame(cleaned_data).to_json()
    return json_str

def eco_driving(time_from, time_to):
    """
    time_from and time_to are unix timestamps for the start and end date
    """

    data = group_unit_items()
    data = list_of_units(data)
    data = data["Unit"].tolist()
    print(data)
    print(len(data))

    result = []

    for d in data:
        # Report execution
        params={
            "reportResourceId":27916207,
            "reportTemplateId":8,
            "reportObjectId":d,
            "reportObjectSecId":0,
            "interval":{
                "from": time_from,
                "to":time_to,
                "flags":0
            }
        }
        url = f"{BASE_URL}svc=report/exec_report&sid={session_id}&sid={session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        
        # fetching report data
        params={
            "tableIndex": 0,
            "rowIndex": 0
        }
        url = f"{BASE_URL}svc=report/get_result_subrows&sid={session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        result.append(response.json())
        print(response.json())
        print(len(result))
    print(result)

    file_path = 'ecodriving_report.json'
    with open(file_path, 'w') as json_file:
        json.dump(result, json_file, indent=4)

    print(f"JSON data has been successfully written to {file_path}")

    cleaned_data = []
    for entry in result:
        for e in entry:
            vehicle_info = {
                'Grouping': e['c'][0],
                'Beginning': e['c'][1]['t'],
                'Initial_Location': e['c'][2]['t'],
                'Rating_By_Violations': e['c'][3],
                'Driver': e['c'][4],
                'Violation': e['c'][5],
                'Value': e['c'][6],
                'Penalties': e['c'][7],
                'Rank': e['c'][8]
            }
            cleaned_data.append(vehicle_info)
    print(vehicle_info)
    print(pd.DataFrame(cleaned_data))
    json_str = pd.DataFrame(cleaned_data).to_json()
    return json_str

def events(time_from, time_to):
    """
    time_from and time_to are unix timestamps for the start and end date
    """

    data = group_unit_items()
    data = list_of_units(data)
    data = data["Unit"].tolist()
    print(data)
    print(len(data))

    result = []

    for d in data:
        # Report execution
        params={
            "reportResourceId":27916207,
            "reportTemplateId":2,
            "reportObjectId":d,
            "reportObjectSecId":0,
            "interval":{
                "from": time_from,
                "to":time_to,
                "flags":0
            }
        }
        url = f"{BASE_URL}svc=report/exec_report&sid={session_id}&sid={session_id}&params={json.dumps(params)}"
        response = requests.get(url)

        # fetching report data
        params={
            "tableIndex": 0,
            "indexFrom": 0,
            "indexTo": 0
        }
        url = f"{BASE_URL}svc=report/get_result_rows&sid={session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        result.append(response.json())
        print(response.json())
        print(len(result))
    print(result)

    file_path = 'events_report.json'
    with open(file_path, 'w') as json_file:
        json.dump(result, json_file, indent=4)

    # cleaned_data = []
    # for entry in result:
    #     vehicle_info = {
    #         'Grouping': entry[0]['c'][0],
    #         'Km/l': entry[0]['c'][1],
    #         'Mileage': entry[0]['c'][2],
    #         'Max_Speed': entry[0]['c'][3]['t'],
    #         'Latitude': entry[0]['c'][3]['y'],
    #         'Longitude': entry[0]['c'][3]['x'],
    #         'Unit': entry[0]['c'][3]['u'],
    #         'Engine_Hours': entry[0]['c'][4],
    #         'Fuel_Consumption': entry[0]['c'][5],
    #         'Initial_Fuel_Level': entry[0]['c'][6],
    #         'Final_Fuel_Level': entry[0]['c'][7],
    #         'Total_Fillings': entry[0]['c'][8],
    #         'Total_Drains': entry[0]['c'][9],
    #         'Fuel_Filled': entry[0]['c'][10],
    #         'Fuel_Drained': entry[0]['c'][11]
    #     }
    #     cleaned_data.append(vehicle_info)
    # print(pd.DataFrame(cleaned_data))

    # json_str = pd.DataFrame(cleaned_data).to_json()
    # return json_str

def group_events(time_from, time_to):
    """
    time_from and time_to are unix timestamps for the start and end date
    """

    data = group_unit_items()
    data = list_of_units(data)
    data = data["Unit"].tolist()
    print(data)
    print(len(data))

    result = []

    for d in data:
        # Report execution
        params={
            "reportResourceId":27916207,
            "reportTemplateId":13,
            "reportObjectId":d,
            "reportObjectSecId":0,
            "interval":{
                "from": time_from,
                "to":time_to,
                "flags":0
            }
        }
        url = f"{BASE_URL}svc=report/exec_report&sid={session_id}&sid={session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        
        # fetching report data
        params={
            "tableIndex": 0,
            "rowIndex": 0
        }
        url = f"{BASE_URL}svc=report/get_result_subrows&sid={session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        result.append(response.json())
        print(response.json())
        print(len(result))
    print(result)

    file_path = 'groupevents_report.json'
    with open(file_path, 'w') as json_file:
        json.dump(result, json_file, indent=4)

    print(f"JSON data has been successfully written to {file_path}")

    cleaned_data = []
    for entry in result:
        for e in entry:
            vehicle_info = {
                'Grouping': e['c'][0],
                'Event_Time': e['c'][1]['t'],
                'Time_Received': e['c'][2],
                'Event_Text': e['c'][3]['t'],
                'Event_Type': e['c'][4],
                'Driver': e['c'][5],
                'Location': e['c'][6]['t']
            }
            cleaned_data.append(vehicle_info)
    print(vehicle_info)
    print(pd.DataFrame(cleaned_data))
    json_str = pd.DataFrame(cleaned_data).to_json()
    return json_str

def eco_driving_v2(time_from, time_to):
    """
    time_from and time_to are unix timestamps for the start and end date
    """

    data = group_unit_items()
    data = list_of_units(data)
    data = data["Unit"].tolist()
    print(data)
    print(len(data))

    result = []

    for d in data:
        # Report execution
        params={
            "reportResourceId":27916207,
            "reportTemplateId":14,
            "reportObjectId":d,
            "reportObjectSecId":0,
            "interval":{
                "from": time_from,
                "to":time_to,
                "flags":0
            },
            "tzOffset": 10800
        }
        url = f"{BASE_URL}svc=report/exec_report&sid={session_id}&sid={session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        
        # fetching report data
        params={
            "tableIndex": 0,
            "rowIndex": 0
        }
        url = f"{BASE_URL}svc=report/get_result_subrows&sid={session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        result.append(response.json())
        print(response.json())
        print(len(result))
    print(result)

    file_path = 'ecodrivingv2_report.json'
    with open(file_path, 'w') as json_file:
        json.dump(result, json_file, indent=4)

    print(f"JSON data has been successfully written to {file_path}")

    cleaned_data = []
    for entry in result:
        try:
            for e in entry:
                vehicle_info = {
                    'Grouping': e['c'][0],
                    'Violation': e['c'][1],
                    'Beginning': e['c'][2]['t'],
                    'Initial_Location': e['c'][3]['t'],
                    'End': e['c'][4]['t'],
                    'Final_Location': e['c'][5]['t'],
                    'Rating_by_Violations': e['c'][6] if e['c'][6] != "-----" else "",
                    'Driver': e['c'][7]
                }
                cleaned_data.append(vehicle_info)
        except:
            pass
    print(vehicle_info)
    print(pd.DataFrame(cleaned_data))
    json_str = pd.DataFrame(cleaned_data).to_json()
    return json_str



# def trips_v2(time_from, time_to):
#     """
#     time_from and time_to are unix timestamps for the start and end date
#     """

#     data = group_unit_items()
#     data = list_of_units(data)
#     data = data["Unit"].tolist()
#     print(data)
#     print(len(data))

#     result = []

#     for d in data:
#         # Report execution
#         params={
#             "reportResourceId":27916207,
#             "reportTemplateId":9,
#             "reportObjectId":d,
#             "reportObjectSecId":0,
#             "interval":{
#                 "from": time_from,
#                 "to":time_to,
#                 "flags":0
#             }
#         }
#         url = f"{BASE_URL}svc=report/exec_report&sid={session_id}&sid={session_id}&params={json.dumps(params)}"
#         response = requests.get(url)
        
#         # fetching report data
#         params={
#             "tableIndex": 0,
#             "indexFrom": 0,
#             "indexTo": 0
#         }
#         url = f"{BASE_URL}svc=report/get_result_rows&sid={session_id}&params={json.dumps(params)}"
#         response = requests.get(url)
#         result.append(response.json())
#         print(response.json())
#         print(len(result))
#     print(result)

#     file_path = 'ecodriving_report.json'
#     with open(file_path, 'w') as json_file:
#         json.dump(result, json_file, indent=4)

#     print(f"JSON data has been successfully written to {file_path}")

#     # cleaned_data = []
#     # for entry in result:
#     #     for e in entry:
#     #         vehicle_info = {
#     #             'Grouping': e['c'][0],
#     #             'Beginning': e['c'][1]['t'],
#     #             'Initial_Location': e['c'][2]['t'],
#     #             'Rating_By_Violations': e['c'][3],
#     #             'Driver': e['c'][4],
#     #             'Violation': e['c'][5],
#     #             'Value': e['c'][6],
#     #             'Penalties': e['c'][7],
#     #             'Rank': e['c'][8]
#     #         }
#     #         cleaned_data.append(vehicle_info)
#     # print(vehicle_info)
#     # print(pd.DataFrame(cleaned_data))


# def detailed_movement(time_from, time_to):
#     """
#     time_from and time_to are unix timestamps for the start and end date
#     """

#     data = group_unit_items()
#     data = list_of_units(data)
#     data = data["Unit"].tolist()
#     print(data)
#     print(len(data))

#     result = []

#     for d in data:
#         # Report execution
#         params={
#             "reportResourceId":27916207,
#             "reportTemplateId":4,
#             "reportObjectId":d,
#             "reportObjectSecId":0,
#             "interval":{
#                 "from": time_from,
#                 "to":time_to,
#                 "flags":0
#             }
#         }
#         url = f"{BASE_URL}svc=report/exec_report&sid={session_id}&sid={session_id}&params={json.dumps(params)}"
#         response = requests.get(url)
        
#         # fetching report data
#         # params={
#         #     "tableIndex": 0,
#         #     "rowIndex": 0
#         # }
#         params={
#             'tableIndex': 0, 
#             'indexFrom': 0, 
#             'indexTo': 0
#         }
#         url = f"{BASE_URL}svc=report/get_result_rows&sid={session_id}&params={json.dumps(params)}"
#         response = requests.get(url)
#         result.append(response.json())
#         print(response.json())
#         print(len(result))
#     print(result)


eco_driving_v2(1717243200, 1721304000)
# group_events(1717243200, 1721304000)
# events(1717243200, 1721304000)
# trips_v2(1717243200, 1721304000)
# eco_driving(1717243200, 1721304000)
# geofence(1717243200, 1721304000)
# detailed_movement(1717243200, 1721304000)
# refueling_and_drops(1717243200, 1721304000)
# print(type(trips(1717243200, 1721304000)))
# summary_report(1717243200, 1721304000)
# result_rows()

# report_data()

# report_tables()

# group_unit_items()
# exec_report()
# # result_rows()
# cleanup_result()

# search_items()

# group_unit_items()



# Example usage
# if __name__ == "__main__":
#     try:
#         # Authenticate and get the session ID
#         session_id = authenticate()
#         print(f"Authenticated successfully. Session ID: {session_id}")
        
#         # Define your parameters for the data request
#         params = {
#             "spec": {
#                 "itemsType": "avl_unit",
#                 "propName": "sys_name",
#                 "propValueMask": "*KC*",
#                 "sortType": "sys_name"
#             },
#             "force": 1,
#             "flags": 1,
#             "from": 0,
#             "to": 0
#         }

#         # Get data
#         data = search_items(session_id, params)
#         print(data)
#     except Exception as e:
#         print(f"Error: {e}")