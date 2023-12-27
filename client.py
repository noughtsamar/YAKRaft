import json
import requests
from datetime import datetime


def create_metadata_record():
    # Code to create a new metadata record
    # ...
    leader_node_urls = ["http://192.168.208.189:8080/send_record","http://192.168.208.50:8080/send_record"]

    with open("cjson.json", 'r') as file:
        record_data = json.load(file)

    selected_indexes = [0,4,5]
    selected_elements = [record_data[i] for i in selected_indexes]

    json_data = {"selected_elements": selected_elements}
    print(json_data)
    # Replace the URL with the actual URL of the leader node

    # all servers must be running
    # Make a POST request to the leader node
   
    for leader_node_url in leader_node_urls:
            try:
                response = requests.post(leader_node_url, json=json_data, timeout=0.1)

# Check if the request was successful

                if response.status_code == 200 and response.json()['status'] == "Record received successfully":
                    print("Record sent successfully to the leader node.")
                    print(" Leader Node url", leader_node_url)
                else:
                    pass
            except requests.exceptions.Timeout:
                continue
            except requests.exceptions.ConnectionError:
                print("Unable to connect to the leader node.", leader_node_url)


def read_metadata_records():
    '''# Code to read existing metadata records
    leader_node_urls_read = ["http://192.168.18.189:8080/read_record","http://192.168.18.50:8080/read_record"]

    try:
        for leader_node_url in leader_node_urls_read:
            response = requests.get(leader_node_url)

    # Check if the request was successful

            if response.status_code == 200 :
                print("Metadata Received is -----")
                print(response)
            else:
                pass
    except requests.exceptions.Timeout:
        print("Request timed out.") 
    except requests.exceptions.ConnectionError:
        print("Unable to connect to the leader node.",leader_node_url)'''


def read_metadata_records():
    leader_node_urls_read = ["http://192.168.208.189:8080/read_record","http://192.168.208.50:8080/read_record"]

    
    for leader_node_url in leader_node_urls_read:
        try:
            response = requests.get(leader_node_url)

# Check if the request was successful (status code 200)
            if (response.status_code == 200):
                data = response.json()
# print(len(data))
                if (len(data) != 0):
                    print("Metadata Received is -----")
                    print(data)

            else:
                print(f"Failed to retrieve metadata. Status code: {response.status_code}")

        except requests.exceptions.Timeout:
            #print("TIMEOUT---------")
            continue 
            
        except requests.exceptions.ConnectionError as e:
            print(f"Unable to connect to the leader node. Error: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

# Call the function
def del_broker(broker_id):
    broker_id_to_delete = 123

# Specify the server URL
    leader_node_urls = ["http://192.168.208.189:8080/delete_record","http://192.168.208.50:8080/delete_record"]
    

# Make a DELETE request to the server
    
    for leader_node_url in leader_node_urls:
            try:
            
                leader_node_url=leader_node_url + "/" + str(broker_id)


                response = requests.delete(leader_node_url)
                # Check if the request was successful (status code 200)
                if (response.status_code == 200 ):
                    if response.json()['status'] == "deleted":
                        print(f"Record for broker ID {broker_id} deleted successfully.")
                        print(response.json())  # Print the server's response

                else:
                    print(f"Failed to delete record. Status code: {response.status_code}")
            except requests.exceptions.Timeout:
                continue
            except requests.exceptions.RequestException as e:
                print(f"Error sending DELETE request: {e}")


while True:
    try:
        user_input = input(
            "Enter 1 to create, 2 to read, 3 to delete (press any other key to exit): ")

        if user_input == '1':
            create_metadata_record()
        elif user_input == '2':
            read_metadata_records()
        elif user_input == '3':
            del_broker(1) 
        else:
            break  # Exit the loop for any other input

    except Exception as e:
        print(f"Error: {e}")

print("Exiting the client.")
