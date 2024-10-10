import gspread
import os
import mysql.connector
from datetime import date
from decimal import Decimal
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

load_dotenv()

db_user = os.getenv('DATABASE_USER')
db_pass = os.getenv('DATABASE_PASS')
db_host = os.getenv('DATABASE_HOST')
db = os.getenv('DATABASE')


# Google Sheets API setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('snipeit-credentials.json', scope)
client = gspread.authorize(creds)
sheet = client.open("SnipeIT Data").sheet1

# Database configuration
db_config = {
    'user': db_user,
    'password': db_pass,
    'host': db_host,
    'database': db,
    'raise_on_warnings': True
}

try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    ranges_to_clear = {
        'A2:E': 5, 
        'J2:P': 7, 
        'R2:T': 3,
        'V2:Y': 4,  
        'AA2:AE': 5, 
        'AG2:AI': 3,
        'AK2:AM': 3,
        'AO2:AP': 2
    }

    for cell_range, col_count in ranges_to_clear.items():
        empty_values = [[""] * col_count for _ in range(1000)]
        sheet.update(range_name=cell_range, values=empty_values)


    # Fetching asset data
    cursor.execute('SELECT a.name AS asset_name, a.serial, a.purchase_date, a.purchase_cost, l.name AS location_name FROM assets a JOIN locations l ON a.location_id = l.id')
    asset_rows = cursor.fetchall()

    formatted_asset_rows = []
    for row in asset_rows:
        formatted_row = []
        for value in row:
            if value is None:
                formatted_row.append("NULL")
            elif isinstance(value, date):
                formatted_row.append(value.strftime('%Y-%m-%d'))
            elif isinstance(value, Decimal):
                formatted_row.append(float(value))
            else:
                formatted_row.append(value)
        formatted_asset_rows.append(formatted_row)

    # Header for asset data
    asset_header = ['Asset Name', 'Serial Number', 'Purchase Date', 'Cost', 'Location']
    if not sheet.cell(1, 1).value:
        sheet.update(range_name='A1:E1', values=[asset_header])

    asset_cell_range = f'A2:E{2 + len(asset_rows) - 1}'
    sheet.update(range_name=asset_cell_range, values=formatted_asset_rows)



    # Fetching maintenance data
    cursor.execute('''
        SELECT am.asset_id, l.name AS location_name, am.asset_maintenance_type, 
            am.title, am.cost, am.completion_date, am.notes
        FROM asset_maintenances am
        JOIN assets a ON am.asset_id = a.id
        JOIN locations l ON a.location_id = l.id
    ''')
    maintenance_rows = cursor.fetchall()

    formatted_maintenance_rows = []
    for row in maintenance_rows:
        formatted_row = []
        for index, value in enumerate(row):
            if index == 0:
                formatted_row.append(str(value).zfill(5))
            elif value is None:
                formatted_row.append("0")
            elif isinstance(value, date):
                formatted_row.append(value.strftime('%Y-%m-%d'))
            elif isinstance(value, Decimal):
                formatted_row.append(float(value))
            else:
                formatted_row.append(value)
        formatted_maintenance_rows.append(formatted_row)

    # Header for maintenance data
    maintenance_header = ['Asset ID', 'Location', 'Maintenance Type', 'Title', 'Cost', 'Date', 'Notes']
    if not sheet.cell(1, 11).value: 
        sheet.update(range_name='J1:P1', values=[maintenance_header])

    maintenance_cell_range = f'J2:P{2 + len(maintenance_rows) - 1}'
    sheet.update(range_name=maintenance_cell_range, values=formatted_maintenance_rows)



    # Fetching license data
    cursor.execute('SELECT name, purchase_cost, seats FROM licenses')
    license_rows = cursor.fetchall()

    formatted_license_rows = []
    for row in license_rows:
        formatted_row = []
        for index, value in enumerate(row):
            if value is None:
                formatted_row.append("NULL")
            elif isinstance(value, date):
                formatted_row.append(value.strftime('%Y-%m-%d'))
            elif isinstance(value, Decimal):
                formatted_row.append(float(value))
            else:
                formatted_row.append(value)
        formatted_license_rows.append(formatted_row)

    # Header for license data
    license_header = ['License Name', 'Cost', 'Seats']
    if not sheet.cell(1, 18).value:
        sheet.update(range_name='R1:T1', values=[license_header])

    license_cell_range = f'R2:T{2 + len(license_rows) - 1}'
    sheet.update(range_name=license_cell_range, values=formatted_license_rows)



    # Fetching accessory data
    cursor.execute('SELECT name, model_number, purchase_cost, qty FROM accessories')
    accessory_rows = cursor.fetchall()

    formatted_accessory_rows = []
    for row in accessory_rows:
        formatted_row = []
        for index, value in enumerate(row):
            if value is None:
                formatted_row.append("NULL")
            elif isinstance(value, date):
                formatted_row.append(value.strftime('%Y-%m-%d'))
            elif isinstance(value, Decimal):
                formatted_row.append(float(value))
            else:
                formatted_row.append(value)
        formatted_accessory_rows.append(formatted_row)

    # Header for accessory data
    accessory_header = ['Accessory Name', 'Model', 'Cost', 'Qty.']
    if not sheet.cell(1, 22).value:
        sheet.update(range_name='V1:Y1', values=[accessory_header])

    accessory_cell_range = f'V2:Y{2 + len(accessory_rows) - 1}'
    sheet.update(range_name=accessory_cell_range, values=formatted_accessory_rows)



    # Fetching consumable data
    cursor.execute('SELECT name, model_number, item_no, purchase_cost, qty FROM consumables')
    consumable_rows = cursor.fetchall()

    formatted_consumable_rows = []
    for row in consumable_rows:
        formatted_row = []
        for index, value in enumerate(row):
            if value is None:
                formatted_row.append("NULL")
            elif isinstance(value, date):
                formatted_row.append(value.strftime('%Y-%m-%d'))
            elif isinstance(value, Decimal):
                formatted_row.append(float(value))
            else:
                formatted_row.append(value)
        formatted_consumable_rows.append(formatted_row)

    # Header for consumable data
    consumable_header = ['Consumable Name', 'Model', 'Item No.', 'Cost', 'Qty.']
    if not sheet.cell(1, 27).value:
        sheet.update(range_name='AA1:AE1', values=[consumable_header])

    consumable_cell_range = f'AA2:AE{2 + len(consumable_rows) - 1}'
    sheet.update(range_name=consumable_cell_range, values=formatted_consumable_rows)



    # Fetching component data
    cursor.execute('SELECT name, serial, purchase_cost FROM components')
    components_rows = cursor.fetchall()

    formatted_components_rows = []
    for row in components_rows:
        formatted_row = []
        for index, value in enumerate(row):
            if value is None:
                formatted_row.append("NULL")
            elif isinstance(value, date):
                formatted_row.append(value.strftime('%Y-%m-%d'))
            elif isinstance(value, Decimal):
                formatted_row.append(float(value))
            else:
                formatted_row.append(value)
        formatted_components_rows.append(formatted_row)

    # Header for component data
    components_header = ['Component Name', 'Serial', 'Cost']
    if not sheet.cell(1, 33).value:
        sheet.update(range_name='AG1:AI1', values=[components_header])

    components_cell_range = f'AG2:AI{2 + len(components_rows) - 1}'
    sheet.update(range_name=components_cell_range, values=formatted_components_rows)



    cursor.execute('''
WITH months AS (
    SELECT DATE_FORMAT(DATE_ADD('2024-01-01', INTERVAL num MONTH), '%Y-%m') AS month,
           DATE_FORMAT(DATE_ADD('2024-01-01', INTERVAL num MONTH), '%M') AS month_name  
    FROM (
        SELECT 0 AS num UNION ALL
        SELECT 1 UNION ALL
        SELECT 2 UNION ALL
        SELECT 3 UNION ALL
        SELECT 4 UNION ALL
        SELECT 5 UNION ALL
        SELECT 6 UNION ALL
        SELECT 7 UNION ALL
        SELECT 8 UNION ALL
        SELECT 9 UNION ALL
        SELECT 10 UNION ALL
        SELECT 11
    ) AS nums
),
locations_months AS (
    SELECT l.id AS location_id, l.name AS location_name, m.month, m.month_name  
    FROM locations l
    CROSS JOIN months m
)
SELECT lm.location_name, lm.month_name,  
       COALESCE(SUM(am.cost), 0) AS total_monthly_cost
FROM locations_months lm
LEFT JOIN assets a ON a.location_id = lm.location_id  
LEFT JOIN asset_maintenances am ON am.asset_id = a.id 
    AND DATE_FORMAT(am.completion_date, '%Y-%m') = lm.month
GROUP BY lm.location_name, lm.month_name  
ORDER BY lm.location_name, MONTH(STR_TO_DATE(lm.month, '%Y-%m'));
    ''')

    # Fetch all rows from the executed query
    monthly_cost_rows = cursor.fetchall()

    # Format the fetched rows for the Google Sheet
    formatted_monthly_cost_rows = []
    for row in monthly_cost_rows:
        formatted_row = [] 
        for value in row:
            if value is None:
                formatted_row.append(0) 
            elif isinstance(value, Decimal):
                formatted_row.append(float(value)) 
            else:
                formatted_row.append(value) 
        formatted_monthly_cost_rows.append(formatted_row) 

    # Header for monthly costs
    monthly_cost_header = ['Location', 'Month', 'Monthly Cost']
    if not sheet.cell(1, 37).value: 
        sheet.update(range_name='AK1:AM1', values=[monthly_cost_header])

    # Calculate the range dynamically based on the data length
    monthly_cost_cell_range = f'AK2:AM{2 + len(monthly_cost_rows) - 1}'
    sheet.update(range_name=monthly_cost_cell_range, values=formatted_monthly_cost_rows)



    # YTD costs by location
    cursor.execute('''
        SELECT l.name AS location_name, SUM(am.cost) AS total_ytd_cost
        FROM asset_maintenances am
        JOIN assets a ON am.asset_id = a.id
        JOIN locations l ON a.location_id = l.id
        WHERE am.completion_date >= CONCAT(YEAR(CURDATE()), '-01-01') AND am.completion_date IS NOT NULL
        GROUP BY location_name
        ORDER BY location_name;
    ''')
    ytd_cost_rows = cursor.fetchall()

    formatted_ytd_cost_rows = []
    for row in ytd_cost_rows:
        formatted_row = []  # Start with an empty row
        for value in row:
            if value is None:
                formatted_row.append(0)
            elif isinstance(value, Decimal):
                formatted_row.append(float(value))
            else:
                formatted_row.append(value)
        formatted_ytd_cost_rows.append(formatted_row)  # Append the entire row

    # Header for YTD costs
    ytd_cost_header = ['Location', 'YTD Cost']
    if not sheet.cell(1, 41).value:  # Check if the header cell is empty
        sheet.update(range_name='AO1:AP1', values=[ytd_cost_header])

    # Calculate the range dynamically based on the data length
    ytd_cost_cell_range = f'AO2:AP{2 + len(ytd_cost_rows) - 1}'
    sheet.update(range_name=ytd_cost_cell_range, values=formatted_ytd_cost_rows)




    print("Google Sheet updated successfully!")

except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    cursor.close()
    conn.close()
