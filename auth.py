import requests
import json
from collections import deque
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()  # This line brings all environment variables from .env into os.environ

# Define your API token and base URL
API_TOKEN = os.environ['API_TOKEN']
BASE_URL = os.environ['BASE_URL']

class Wialon:
    def __init__(self, api_token, base_url):
        self.api_token = api_token
        self.base_url = base_url
        self.session_id = self.authenticate()

    @staticmethod
    def remove_keys(d, keys_to_remove):
        if isinstance(d, dict):
            # Remove each key if it exists in the current dictionary level
            for key in keys_to_remove:
                if key in d:
                    del d[key]
            # Recursively call this function for each value in the dictionary
            for key in list(d.keys()):  # Use list(d.keys()) to avoid 'dictionary changed size during iteration' error
                Wialon.remove_keys(d[key], keys_to_remove)
        elif isinstance(d, list):
            # If the dictionary contains lists, recursively call this function for each item in the list
            for item in d:
                Wialon.remove_keys(item, keys_to_remove)

    @staticmethod
    def rename_keys(d, key_map):
        new_data = {}
        for old_key, value in d.items():
            new_value = {}
            for inner_key, inner_value in value.items():
                new_key = key_map.get(inner_key, inner_key)
                new_value[new_key] = inner_value
            new_data[old_key] = new_value
        return new_data

    @staticmethod
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

    # Function to authenticate and get a session ID
    def authenticate(self):
        url = f"{self.base_url}svc=token/login&params={{\"token\":\"{self.api_token}\"}}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if 'error' not in data:
                return data['eid']  # Extract the session ID from the response
            else:
                raise Exception(f"Error in authentication: {data['error']}")
        else:
            raise Exception(f"Failed to connect to server: {response.status_code}")

    def cleanup_result(self):
        # session_id = authenticate()
        url = "https://hst-api.wialon.com/wialon/ajax.html?svc=report/cleanup_result&params={}&sid=self.session_id"
        response = requests.get(url)
        return response.json()

    # Function to get data
    def search_items(self):
        params = {"spec":{"itemsType":"avl_unit_group","propName":"","propValueMask":"","sortType":"","propType":"","or_logic":False},"force":1,"flags":1,"from":0,"to":0}
        url = f"{self.base_url}svc=core/search_items&sid=self.session_id&params={json.dumps(params)}"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to retrieve data: {response.status_code}")
        
    def group_unit_items(self):
        params = {"spec":{"itemsType":"avl_unit_group","propName":"","propValueMask":"","sortType":"","propType":"","or_logic":False},"force":1,"flags":1,"from":0,"to":0}
        url = f"{self.base_url}svc=core/search_items&sid={self.session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        if response.status_code == 200:
            response = dict(response.json())
            response = response.get("items")[1:]
            keys_to_remove = ["cls", "mu", "uacl"]
            
            self.remove_keys(response, keys_to_remove)
            response = {i: d for i, d in enumerate(response, start=1)}

            key_map = {"nm": "unit_group", "id": "unit_group_id", "u": "units"}
            return self.rename_keys(response, key_map)
        else:
            raise Exception(f"Failed to retrieve data: {response.status_code}")

    def search_unit_type(self):
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
        url = f"{self.base_url}svc=core/search_items&sid={self.session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to retrieve data: {response.status_code}")
        
    def search_unit_groups(self):
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
        url = f"{self.base_url}svc=core/search_items&sid={self.session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to retrieve data: {response.status_code}")
        
    def exec_report(self):
        params={
            "reportResourceId":27916207,
            "reportTemplateId":2,
            "reportObjectId":27922767,
            "reportObjectSecId":0,
            "interval":{
                "from": 1717243200,
                "to":1721304000,
                "flags":0
            }
        }
        url = f"{self.base_url}svc=report/exec_report&sid={self.session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        
        params={
            "tableIndex": 0,
            "indexFrom": 0,
            "indexTo": 0
        }
        url = f"{self.base_url}svc=report/get_result_rows&sid={self.session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        return response.json()
        
    def report_tables(self):
        params={

        }
        url = f"{self.base_url}svc=report/get_report_tables&sid={self.session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        if response.status_code == 200:
            response = response.json()
            return response
        else:
            raise Exception(f"Failed to retrieve data: {response.status_code}")
        
    def report_data(self):
        params={
            "itemId": 27916207,
            "col": [1,2,3,4,5,6,7,8,9,10,11,12,13,14],
            "flags": 0
        }
        url = f"{self.base_url}svc=report/get_report_data&sid={self.session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        if response.status_code == 200:
            response = response.json()
            return response
        else:
            raise Exception(f"Failed to retrieve data: {response.status_code}")
        
    def result_rows(self):
        params={
            "tableIndex": 0,
            "indexFrom": 0,
            "indexTo": 0
        }
        url = f"{self.base_url}svc=report/get_result_rows&sid={self.session_id}&params={json.dumps(params)}"
        response = requests.get(url)
        return response.json()

    def summary_report(self, time_from, time_to):
        """
        time_from and time_to are unix timestamps for the start and end date
        """

        data = self.group_unit_items()
        data = self.list_of_units(data)
        data = list(set(data["Unit"].tolist()))
        # print(data)
        # print(len(data))

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
            url = f"{self.base_url}svc=report/exec_report&sid={self.session_id}&params={json.dumps(params)}"
            response = requests.get(url)

            # fetching report data
            params={
                "tableIndex": 0,
                "indexFrom": 0,
                "indexTo": 0
            }
            url = f"{self.base_url}svc=report/get_result_rows&sid={self.session_id}&params={json.dumps(params)}"
            response = requests.get(url)
            result.append(response.json())
        #     print(response.json())
        #     print(len(result))
        # print(result)

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
        # print(pd.DataFrame(cleaned_data))
        json_str = pd.DataFrame(cleaned_data).to_json()
        return json_str

    def trips(self, time_from, time_to):
        """
        time_from and time_to are unix timestamps for the start and end date
        """

        data = self.group_unit_items()
        data = self.list_of_units(data)
        data = list(set(data["Unit"].tolist()))
        # print(data)
        # print(len(data))

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
            url = f"{self.base_url}svc=report/exec_report&sid={self.session_id}&params={json.dumps(params)}"
            response = requests.get(url)
            
            # fetching report data
            params={
                "tableIndex": 0,
                "rowIndex": 0
            }
            url = f"{self.base_url}svc=report/get_result_subrows&sid={self.session_id}&params={json.dumps(params)}"
            response = requests.get(url)
            result.append(response.json())
        #     print(response.json())
        #     print(len(result))
        # print(result)

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

    def refueling_and_drops(self, time_from, time_to):
        """
        time_from and time_to are unix timestamps for the start and end date
        """

        data = self.group_unit_items()
        data = self.list_of_units(data)
        data = list(set(data["Unit"].tolist()))
        # print(data)
        # print(len(data))

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
            url = f"{self.base_url}svc=report/exec_report&sid={self.session_id}&params={json.dumps(params)}"
            response = requests.get(url)
            
            # fetching report data
            params={
                "tableIndex": 0,
                "rowIndex": 0
            }
            url = f"{self.base_url}svc=report/get_result_subrows&sid={self.session_id}&params={json.dumps(params)}"
            response = requests.get(url)
            result.append(response.json())
        #     print(response.json())
        #     print(len(result))
        # print(result)

        # file_path = 'vehicle_data.json'
        # with open(file_path, 'w') as json_file:
        #     json.dump(result, json_file, indent=4)

        # print(f"JSON data has been successfully written to {file_path}")

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
        # data.to_csv('refuel_and_drops.csv', index=False)
        print(f"DataFrame has been successfully written to refuel_and_drops.csv")
        return data.to_json()

    def geofence(self, time_from, time_to):
        """
        time_from and time_to are unix timestamps for the start and end date
        """

        data = self.group_unit_items()
        data = self.list_of_units(data)
        data = list(set(data["Unit"].tolist()))
        # print(data)
        # print(len(data))

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
            url = f"{self.base_url}svc=report/exec_report&sid={self.session_id}&params={json.dumps(params)}"
            response = requests.get(url)
            
            # fetching report data
            params={
                "tableIndex": 0,
                "rowIndex": 0
            }
            url = f"{self.base_url}svc=report/get_result_subrows&sid={self.session_id}&params={json.dumps(params)}"
            response = requests.get(url)
            result.append(response.json())
        #     print(response.json())
        #     print(len(result))
        # print(result)

        # file_path = 'geofence_report.json'
        # with open(file_path, 'w') as json_file:
        #     json.dump(result, json_file, indent=4)

        # print(f"JSON data has been successfully written to {file_path}")

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
        # print(vehicle_info)
        # print(pd.DataFrame(cleaned_data))

        json_str = pd.DataFrame(cleaned_data).to_json()
        return json_str

    def eco_driving(self, time_from, time_to):
        """
        time_from and time_to are unix timestamps for the start and end date
        """

        data = self.group_unit_items()
        data = self.list_of_units(data)
        data = list(set(data["Unit"].tolist()))
        # print(data)
        # print(len(data))

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
            url = f"{self.base_url}svc=report/exec_report&sid={self.session_id}&params={json.dumps(params)}"
            response = requests.get(url)
            
            # fetching report data
            params={
                "tableIndex": 0,
                "rowIndex": 0
            }
            url = f"{self.base_url}svc=report/get_result_subrows&sid={self.session_id}&params={json.dumps(params)}"
            response = requests.get(url)
            result.append(response.json())
        #     print(response.json())
        #     print(len(result))
        # print(result)

        # file_path = 'ecodriving_report.json'
        # with open(file_path, 'w') as json_file:
        #     json.dump(result, json_file, indent=4)

        # print(f"JSON data has been successfully written to {file_path}")

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
        # print(vehicle_info)
        # print(pd.DataFrame(cleaned_data))
        json_str = pd.DataFrame(cleaned_data).to_json()
        return json_str

    def events(self, time_from, time_to):
        """
        time_from and time_to are unix timestamps for the start and end date
        """

        data = self.group_unit_items()
        data = self.list_of_units(data)
        data = list(set(data["Unit"].tolist()))
        # print(data)
        # print(len(data))

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
            url = f"{self.base_url}svc=report/exec_report&sid={self.session_id}&params={json.dumps(params)}"
            response = requests.get(url)

            # fetching report data
            params={
                "tableIndex": 0,
                "indexFrom": 0,
                "indexTo": 0
            }
            url = f"{self.base_url}svc=report/get_result_rows&sid={self.session_id}&params={json.dumps(params)}"
            response = requests.get(url)
            result.append(response.json())
        #     print(response.json())
        #     print(len(result))
        # print(result)

        # file_path = 'events_report.json'
        # with open(file_path, 'w') as json_file:
        #     json.dump(result, json_file, indent=4)
    
    def group_events(self, time_from, time_to):
        """
        time_from and time_to are unix timestamps for the start and end date
        """

        data = self.group_unit_items()
        data = self.list_of_units(data)
        data = list(set(data["Unit"].tolist()))
        # print(data)
        # print(len(data))

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
            url = f"{self.base_url}svc=report/exec_report&sid={self.session_id}&params={json.dumps(params)}"
            response = requests.get(url)
            
            # fetching report data
            params={
                "tableIndex": 0,
                "rowIndex": 0
            }
            url = f"{self.base_url}svc=report/get_result_subrows&sid={self.session_id}&params={json.dumps(params)}"
            response = requests.get(url)
            result.append(response.json())
        #     print(response.json())
        #     print(len(result))
        # print(result)

        # file_path = 'groupevents_report.json'
        # with open(file_path, 'w') as json_file:
        #     json.dump(result, json_file, indent=4)

        # print(f"JSON data has been successfully written to {file_path}")

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
        # print(vehicle_info)
        print(pd.DataFrame(cleaned_data))
        json_str = pd.DataFrame(cleaned_data).to_json()
        return json_str

    def eco_driving_v2(self, time_from, time_to):
        """
        time_from and time_to are unix timestamps for the start and end date
        """

        data = self.group_unit_items()
        data = self.list_of_units(data)
        data = list(set(data["Unit"].tolist()))
        # print(data)
        # print(len(data))

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
            url = f"{self.base_url}svc=report/exec_report&sid={self.session_id}&params={json.dumps(params)}"
            response = requests.get(url)
            
            # fetching report data
            params={
                "tableIndex": 0,
                "rowIndex": 0
            }
            url = f"{self.base_url}svc=report/get_result_subrows&sid={self.session_id}&params={json.dumps(params)}"
            response = requests.get(url)
            result.append(response.json())
        #     print(response.json())
        #     print(len(result))
        # print(result)

        # file_path = 'ecodrivingv2_report.json'
        # with open(file_path, 'w') as json_file:
        #     json.dump(result, json_file, indent=4)

        # print(f"JSON data has been successfully written to {file_path}")

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
        # print(vehicle_info)
        print(pd.DataFrame(cleaned_data))
        json_str = pd.DataFrame(cleaned_data).to_json()
        return json_str

# Instantiate the class
api = Wialon(API_TOKEN, BASE_URL)
result = api.eco_driving_v2(1717243200, 1721304000)
print(result)