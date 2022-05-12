import json

# Open the JSON file for conversion
with open('scams.json') as json_file:
    json = json.load(json_file)

# Define our output csv file
csv_output_file = open('scams.csv', 'w')

# Iterate through our JSON and extract required data
for address in json['result'].keys():
    for i in json['result'][address]['addresses']:

        # Extract the name
        name = str(json['result'][address]['name'])
        # Extract the URL
        url = str(json['result'][address]['url'])
        # Extract the coin type
        coin = str(json['result'][address]['coin'])
        # Extract the categories
        if json['result'][address]['category'] != 'Scam':
          category = str(json['result'][address]['category'])
        else:
          category = 'Scamming'
        # Extract the subcategories
        if 'subcategory' in json['result'][address]:
          subcategory = str(json['result'][address]['subcategory'])
        else:
          subcategory = ""
        # Extract the index
        index = str(i)
        # Extract the status
        status = str(json['result'][address]['status'])

        # Construct the csv line
        csv_line = str(address) + ',' + name + ',' + url + ',' + coin + ',' + category + ',' + subcategory + ',' + index + ',' + status + '\n'

        # Add the csv line to the output
        csv_output_file.write(csv_line)

csv_output_file.close()
