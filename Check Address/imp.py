import pyodbc
import requests

# Function to get coordinates from Google Maps API
def get_coordinates(address, api_key):
    url = f'https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={api_key}'
    response = requests.get(url)
    data = response.json()
    if data['status'] == 'OK':
        lat = data['results'][0]['geometry']['location']['lat']
        lng = data['results'][0]['geometry']['location']['lng']
        return lat, lng
    return None, None

# Set up the connection to SQL Server
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.10.4.100;DATABASE=Hamza;UID=sa;PWD=0000')
cursor = conn.cursor()

# Replace with your SQL query
cursor.execute("SELECT BillingAddress1,LagBillingAddress FROM #tblFirstFilter WHERE	LEFT(BillingAddress1, CHARINDEX(' ', BillingAddress1 + ' ') - 1) <> LEFT(LagBillingAddress, CHARINDEX(' ', LagBillingAddress + ' ') - 1)")
rows = cursor.fetchall()

# Replace with your Google API key
api_key = 'YOUR_GOOGLE_API_KEY'

for row in rows:
    address1 = row.BillingAddress1
    address2 = row.LagBillingAddress

    # Get coordinates for both addresses
    lat1, lng1 = get_coordinates(address1, api_key)
    lat2, lng2 = get_coordinates(address2, api_key)

    # Compare the coordinates
    if lat1 == lat2 and lng1 == lng2:
        print(f"The addresses '{address1}' and '{address2}' are the same location.")
    else:
        print(f"The addresses '{address1}' and '{address2}' are different locations.")
        
        
        
        
        
        
