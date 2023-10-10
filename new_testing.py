from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta
import math
import pyodbc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import logging


server = 'samplegrowlocserver.database.windows.net'
database = 'samplegrowlocdb'
username = 'samplegrowloc'
password = 'Admin123'
driver = '{ODBC Driver 18 for SQL Server}'

conn = pyodbc.connect(f'SERVER={server};DATABASE={database};UID={username};PWD={password};DRIVER={driver}')
cursor = conn.cursor()

################################################################################################################

def initialize_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('log-level=3')
    options.add_argument('headless')
    return webdriver.Chrome(options=options)

################################################################################################################

def commodity_create_table(row):
    
    table_exists_query = f"SELECT COUNT(*) FROM sys.objects WHERE object_id = OBJECT_ID('{table_name}') AND type = 'U';"
    cursor.execute(table_exists_query)
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        create_table_sql = f"CREATE TABLE {table_name} ({id_name} INT IDENTITY(1,1) PRIMARY KEY, commodity VARCHAR(255), parent_commodity VARCHAR(255), variety VARCHAR(255), state VARCHAR(255), district VARCHAR(255), mandi VARCHAR(255), [min_price(/Qui)] int, [avg_price(/Qui)] int ,[max_price(/Qui)] int, arrival_date DATE,source VARCHAR(255),temperature VARCHAR(255),humidity VARCHAR(255), weather VARCHAR(255));"
        cursor.execute(create_table_sql)
        conn.commit()

################################################################################################################

def commodity_insert_table(data_dicts):
    
    for row in data_dicts:
        check_exists_query = f"SELECT COUNT(*) FROM {table_name} WHERE commodity = ? AND parent_commodity = ? AND variety = ? AND state = ? AND district = ? AND mandi = ? AND [min_price(/Qui)] = ? AND [avg_price(/Qui)] = ? AND [max_price(/Qui)] = ? AND arrival_date = ? AND source = ? AND temperature = ? AND humidity = ? AND weather = ?;"
        cursor.execute(check_exists_query, row['commodity'], row['parent_commodity'], row['variety'], row['state'], row['district'], row['mandi'], row['min_price(/Qui)'], row['avg_price(/Qui)'], row['max_price(/Qui)'], row['arrival_date'], row['source'], row['temperature'], row['humidity'], row['weather'])
        row_exists = cursor.fetchone()[0]

        if not row_exists:
            insert_sql = f"INSERT INTO {table_name} (commodity, parent_commodity, variety, state, district, mandi, [min_price(/Qui)], [avg_price(/Qui)], [max_price(/Qui)], arrival_date, source, temperature, humidity, weather) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            cursor.execute(insert_sql, row['commodity'], row['parent_commodity'], row['variety'], row['state'], row['district'], row['mandi'], row['min_price(/Qui)'], row['avg_price(/Qui)'], row['max_price(/Qui)'], row['arrival_date'], row['source'], row['temperature'], row['humidity'], row['weather'])
            conn.commit()

################################################################################################################

def weather_create_table(row):
    
    table_exists_query = f"SELECT COUNT(*) FROM sys.objects WHERE object_id = OBJECT_ID('{table_name}') AND type = 'U';"
    cursor.execute(table_exists_query)
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        create_table_sql = f"CREATE TABLE {table_name} ({id_name} INT IDENTITY(1,1) PRIMARY KEY, state VARCHAR(255), district VARCHAR(255), mandi VARCHAR(255), temperature VARCHAR(255), humidity VARCHAR(255), weather VARCHAR(255), date DATE, source VARCHAR(255));"
        cursor.execute(create_table_sql)
        conn.commit()


################################################################################################################

def weather_insert_table(data_dicts):
    
    for row in data_dicts:
        check_exists_query = f"SELECT COUNT(*) FROM {table_name} WHERE state = ? AND district = ? AND mandi = ? AND date = ? AND source = ?;"
        cursor.execute(check_exists_query, row['state'], row['district'], row['mandi'], row['date'], row['source'])
        row_exists = cursor.fetchone()[0]

        if not row_exists:
            insert_sql = f"INSERT INTO {table_name} (state, district, mandi, temperature, humidity, weather, date, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            cursor.execute(insert_sql, row['state'], row['district'], row['mandi'], row['temperature'], row['humidity'], row['weather'], row['date'], row['source'])
            conn.commit()

################################################################################################################

def mandi_create_table(row):
    
    table_exists_query = f"SELECT COUNT(*) FROM sys.objects WHERE object_id = OBJECT_ID('{table_name}') AND type = 'U';"
    cursor.execute(table_exists_query)
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        create_table_sql = f"CREATE TABLE {table_name} ({id_name} INT IDENTITY(1,1) PRIMARY KEY, state VARCHAR(255), district VARCHAR(255), mandi VARCHAR(255), source VARCHAR(255));"
        cursor.execute(create_table_sql)
        conn.commit()

################################################################################################################

def mandi_insert_table(data_dicts):
    
    for row in data_dicts:
        check_exists_query = f"SELECT COUNT(*) FROM {table_name} WHERE state = ? AND district = ? AND mandi = ? AND source = ?;"
        cursor.execute(check_exists_query, row['state'], row['district'], row['mandi'], row['source'])
        row_exists = cursor.fetchone()[0]

        if not row_exists:
            insert_sql = f"INSERT INTO {table_name} (state, district, mandi, source) VALUES (?, ?, ?, ?)"
            cursor.execute(insert_sql, row['state'], row['district'], row['mandi'], row['source'])
            conn.commit()

################################################################################################################

def commodity_dictionary_data(commodity, parent_commodity, variety, state, district, mandi, min_price, avg_price, max_price, arrival_date, source, temperature, humidity, weather):
    data_dict = {
        'commodity': commodity,
        'parent_commodity': parent_commodity,
        'variety': variety,
        'state': state,
        'district': district,
        'mandi': mandi,
        'min_price(/Qui)': min_price,
        'avg_price(/Qui)': avg_price,
        'max_price(/Qui)': max_price,
        'arrival_date': arrival_date,
        'source': source,
        'temperature': temperature,
        'humidity': humidity,
        'weather': weather
    }
    return data_dict

################################################################################################################

def weather_dictionary_data(state, district, mandi, temperature, humidity, weather, date, source):
    data_dict = {
        'state' : state,
        'district' : district,
        'mandi' : mandi,
        'temperature' : temperature,
        'humidity' : humidity,
        'weather' : weather,
        'date': date,
        'source' : source
    }
    return data_dict

################################################################################################################

def mandi_dictionary_data(state, district, mandi, source):
    data_dict = {
        'state' : state,
        'district' : district,
        'mandi' : mandi,
        'source' : source
    }
    return data_dict

################################################################################################################

def update_view():
    print("update_view function called")
    view_query = """
        CREATE OR ALTER VIEW commodity_view_testing AS
        SELECT 
            LOWER(sample1.commodity) as commodity, LOWER(sample1.parent_commodity) as parent_commodity, LOWER(sample1.variety) as variety, LOWER(sample1.state) as state, 
            COALESCE(
                LOWER((SELECT TOP 1 m1.district FROM dim_mandi_testing m1
                        WHERE m1.state = sample1.state AND m1.mandi = sample1.mandi)),
                LOWER(sample1.district)
            ) as district,
            LOWER(sample1.mandi) as mandi, sample1.[min_price(/Qui)], sample1.[avg_price(/Qui)], sample1.[max_price(/Qui)], sample1.arrival_date, LOWER(sample1.source) as source,
            LOWER(CASE 
                    WHEN sample1.source = 'commodity_online' THEN 
                        (SELECT TOP 1 w1.temperature FROM dim_weather_testing w1
                            WHERE w1.date = sample1.arrival_date AND (w1.mandi = sample1.mandi OR (w1.district = sample1.district AND w1.state = sample1.state))
                        )
                    WHEN sample1.source = 'eNAM' THEN 
                        (SELECT TOP 1 w1.temperature FROM dim_weather_testing w1
                            WHERE w1.date = sample1.arrival_date AND (w1.district = (
                                SELECT TOP 1 m1.district FROM dim_mandi_testing m1 WHERE m1.state = sample1.state AND m1.mandi = sample1.mandi) AND w1.state = sample1.state)
                        )
                END
            ) as temperature,
            LOWER(CASE 
                    WHEN sample1.source = 'commodity_online' THEN 
                        (SELECT TOP 1 w1.humidity FROM dim_weather_testing w1
                            WHERE w1.date = sample1.arrival_date AND (w1.mandi = sample1.mandi OR (w1.district = sample1.district AND w1.state = sample1.state))
                        )
                    WHEN sample1.source = 'eNAM' THEN 
                        (SELECT TOP 1 w1.humidity FROM dim_weather_testing w1
                            WHERE w1.date = sample1.arrival_date AND (w1.district = (
                                SELECT TOP 1 m1.district FROM dim_mandi_testing m1 WHERE m1.state = sample1.state AND m1.mandi = sample1.mandi) AND w1.state = sample1.state)
                        )
                END
            ) as humidity
        FROM (
            SELECT * FROM dim_commodityonline_testing
            UNION ALL
            SELECT * FROM dim_eNAM_testing
        ) sample1;
    """

    cursor.execute(view_query)
    conn.commit()
    print("update_view function exited")
    pass

################################################################################################################

def commodityonline_code(row):
    print("commodityonline code running .....")

    commodity_create_table(row)
    driver = initialize_driver()
    driver.get(source_url)

    my_list = ['Spinach', 'Tomato', 'Cabbage', 'Capsicum', 'Chilly-Capsicum', 'Cauliflower', 'Cucumbar-Kheera', 'Coriander-Leaves', 'Corriander-seed', 'Chili-Red', 'Dry-Chillies', 'Green-Chilli', 'Water-Melon']

    for element in my_list:
        select_commodity = f"document.getElementById('commodity-list').value = '{element}';"
        click_search = "document.getElementById('searchmandi').click();"
        driver.execute_script(select_commodity + click_search)

        updated_url = f"https://www.commodityonline.com/mandiprices/{element.lower()}"
        driver.get(updated_url)
        time.sleep(2)

        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'lxml')
        table = soup.find("table", class_=f"{source_table_class}")
        rows = table.find_all("tr")
        
        data_dicts = []
        for row in rows[1:]:
            cells = row.find_all("td")
            row = [tr.text for tr in cells]
            row = [y.replace('\n', '') for y in row]
            row = row[:-1]

            commodity = cells[0].text.strip()

            if commodity == 'Spinach': parent_commodity = 'Spinach'
            elif commodity == 'Tomato': parent_commodity = 'Tomato'
            elif commodity == 'Capsicum' or commodity == 'Chilly Capsicum': parent_commodity = 'Capsicum'
            elif commodity == 'Cauliflower': parent_commodity = 'Cauliflower'
            elif commodity == 'Cucumbar(Kheera)': parent_commodity = 'Cucumbar'
            elif commodity == 'Coriander(Leaves)' or commodity == 'Corriander seed': parent_commodity = 'Coriander'
            elif commodity in ['Chili Red', 'Dry Chillies', 'Green Chilli']: parent_commodity = 'Chilli'
            elif commodity == 'Water Melon': parent_commodity = 'Water Melon'
            elif commodity == 'Cabbage': parent_commodity = 'Cabbage'
            else: parent_commodity = '0'

            variety = cells[2].text.strip()
            state = cells[3].text.strip()
            district = cells[4].text.strip()
            mandi = cells[5].text.strip()
            min_price = int(cells[6].text.strip().replace(',', '').split()[1]) if cells[6].text.strip().replace(',', '') != '0' else 0
            avg_price = int(cells[8].text.strip().replace(',', '').split()[1]) if cells[6].text.strip().replace(',', '') != '0' else 0
            max_price = int(cells[7].text.strip().replace(',', '').split()[1]) if cells[6].text.strip().replace(',', '') != '0' else 0
            arrival_date = datetime.strptime(cells[1].text.strip(), "%d/%m/%Y").date()
            source = f"{source_name}"
            temperature = '0'
            humidity = '0'
            weather = '0'

            data_dict = commodity_dictionary_data(commodity, parent_commodity, variety, state, district, mandi, min_price, avg_price, max_price, arrival_date, source, temperature, humidity, weather)
            data_dicts.append(data_dict)
        commodity_insert_table(data_dicts)

    pass

##################################################### eNAM ##########################################################

def eNAM_code(row):
    print("eNAM code running .....")

    commodity_create_table(row)
    driver = initialize_driver()
    driver.get(source_url)

    date_input = driver.find_element(By.ID, "min_max_apmc_from_date")
    date_input.clear()
    time.sleep(7)
    current_date = datetime.now().date()
    yesterday_date = current_date - timedelta(days=1)
    yesterday_date = yesterday_date.strftime('%Y-%m-%d')
    driver.execute_script("arguments[0].value = arguments[1]", date_input, yesterday_date)
    driver.execute_script("arguments[0].dispatchEvent(new Event('change'))", date_input)

    time.sleep(1)

    dropdown_element = driver.find_element('id','min_max_state')
    select = Select(dropdown_element)
    select.select_by_visible_text('MAHARASHTRA')
    time.sleep(1)
    select.select_by_visible_text('-- All --')
    time.sleep(1)

    commodity_element = driver.find_element('id','min_max_commodity')
    select_commodity = Select(commodity_element)
    options = select_commodity.options
    option_list = [option.text for option in options]
    time.sleep(1)
    my_list = ['SPINACH (PALAK)', 'TOMATO', 'CAPSICUM', 'BROCCOLI', 'CAULIFLOWER', 'CUCUMBER', 'CORANDER LEAVES', 'CORIANDAR', 'CORIANDER WHOLE', 'CORIANDER LEAVES', 'CHILLI BADIGA', 'CHILLI BANGARAM', 'CHILLI NS 250 BULLET', 'CHILLI TEJA A/C', 'CHILLI WHITE', 'CHILLI-273', 'CHILLI-273 A/C', 'CHILLI-334', 'CHILLI-334 A/C', 'CHILLI-341', 'CHILLI-341 A/C', 'CHILLI-4884', 'CHILLI-4884 A/C', 'CHILLI-5', 'CHILLI-5 A/C', 'CHILLI-BADIGI A/C', 'CHILLI-CHANDRAMUKHI', 'CHILLI-CHANDRAMUKHI A/C', 'CHILLI-DEEPIKA', 'CHILLI-DELUX', 'CHILLI-DESI', 'CHILLI-DEVANURU DELUX', 'CHILLI-DEVANURU DELUX A/C', 'CHILLI-G.T', 'CHILLI-GUNTUR SANNAM', 'CHILLI-NANDINI', 'CHILLI-NANDINI A/C', 'CHILLI-RED TOP', 'CHILLI-SINGLE PATTI', 'CHILLI-SUPER10', 'CHILLI-SUPER10 A/C', 'CHILLI-TALU A/C', 'CHILLI-TEJA', 'CHILLI-THAALU', 'CHILLI-US341', 'CHILLI-WONDERHOT', 'CHILLI-WONDERHOT A/C', 'CHILLIES', 'DRY CHILLY_10 KG(TN)', 'RED CHILLI-DRY', 'RED CHILLI-MOISTURED', 'CABBAGE', 'BUTTON MUSHROOM', 'OYSTER MUSHROOM', 'OYSTER MUSHROOM DRIED', 'OYSTER MUSHROOM FRESH', 'WATER MELON']

    for element in my_list:
        if element in option_list:
            select_commodity.select_by_visible_text(element)
            submit_button = driver.find_element('id', 'refresh')
            submit_button.click()
            time.sleep(2)

            select_element = driver.find_element(By.ID, "min_max_no_of_list")
            options = select_element.find_elements(By.TAG_NAME, "option")
            option_value_map = {option.text: option.get_attribute('value') for option in options}
            sorted_options = sorted(option_value_map.keys(), key=lambda x: int(x))
            # print(sorted_options)

            select_page = Select(select_element)

            for page_number in sorted_options:
                option_value = option_value_map[page_number]
                select_page.select_by_value(str(option_value))

                time.sleep(2)
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'lxml')
                table = soup.find("table", class_=f"{source_table_class}")
                rows = table.find_all("tr")
                
                data_dicts = []
                for row in rows[2:]:
                    cells = row.find_all("td")
                    
                    commodity = cells[2].text.strip()

                    if commodity == 'SPINACH (PALAK)': parent_commodity = 'Spinach'
                    elif commodity == 'TOMATO': parent_commodity = 'Tomato'
                    elif commodity == 'CAPSICUM': parent_commodity = 'Capsicum'
                    elif commodity == 'BROCCOLI': parent_commodity = 'Broccoli'
                    elif commodity == 'CAULIFLOWER': parent_commodity = 'Cauliflower'
                    elif commodity == 'CUCUMBER': parent_commodity = 'Cucumber'
                    elif commodity in ['CORANDER LEAVES', 'CORIANDAR', 'CORIANDER WHOLE', 'CORIANDER LEAVES']: parent_commodity = 'Coriander'
                    elif commodity in ['CHILLI BADIGA', 'CHILLI BANGARAM', 'CHILLI NS 250 BULLET', 'CHILLI TEJA A/C', 'CHILLI WHITE', 'CHILLI-273', 'CHILLI-273 A/C', 'CHILLI-334', 'CHILLI-334 A/C', 'CHILLI-341', 'CHILLI-341 A/C', 'CHILLI-4884', 'CHILLI-4884 A/C', 'CHILLI-5', 'CHILLI-5 A/C', 'CHILLI-BADIGI A/C', 'CHILLI-CHANDRAMUKHI', 'CHILLI-CHANDRAMUKHI A/C', 'CHILLI-DEEPIKA', 'CHILLI-DELUX', 'CHILLI-DESI', 'CHILLI-DEVANURU DELUX', 'CHILLI-DEVANURU DELUX A/C', 'CHILLI-G.T', 'CHILLI-GUNTUR SANNAM', 'CHILLI-NANDINI', 'CHILLI-NANDINI A/C', 'CHILLI-RED TOP', 'CHILLI-SINGLE PATTI', 'CHILLI-SUPER10', 'CHILLI-SUPER10 A/C', 'CHILLI-TALU A/C', 'CHILLI-TEJA', 'CHILLI-THAALU', 'CHILLI-US341', 'CHILLI-WONDERHOT', 'CHILLI-WONDERHOT A/C', 'CHILLIES', 'DRY CHILLY_10 KG(TN)', 'RED CHILLI-DRY', 'RED CHILLI-MOISTURED']: parent_commodity = 'Chilli'
                    elif commodity == 'CABBAGE': parent_commodity = 'Cabbage'
                    elif commodity in ['BUTTON MUSHROOM', 'OYSTER MUSHROOM', 'OYSTER MUSHROOM DRIED', 'OYSTER MUSHROOM FRESH']: parent_commodity = 'Mushroom'
                    elif commodity == 'WATER MELON': parent_commodity = 'Water Melon'
                    else: parent_commodity = '0'

                    variety = '0'
                    state = cells[0].text.strip()
                    district = '0'
                    mandi = cells[1].text.strip()
                    unit = cells[8].text.strip()
                    def process_price_cell(cell):
                        price = cell.text.strip().replace(',', '')
                        return str(int(price) * 100) if unit == 'Kg' else str(int(price) * 2) if unit == '50 Kg' else price

                    min_price = process_price_cell(cells[3])
                    avg_price = process_price_cell(cells[4])
                    max_price = process_price_cell(cells[5])
                    arrival_date = datetime.strptime(cells[9].text.strip(), "%d-%m-%Y").date()
                    source = f'{source_name}'
                    temperature = '0'
                    humidity = '0'
                    weather = '0'

                    data_dict = commodity_dictionary_data(commodity, parent_commodity, variety, state, district, mandi, min_price, avg_price, max_price, arrival_date, source, temperature, humidity, weather)
                    data_dicts.append(data_dict)
                commodity_insert_table(data_dicts)

    pass

################################################################################################################

def mausam_code(row):
    print("mausam code running .....")

    weather_create_table(row)
    driver = initialize_driver()
    driver.get(source_url)

    state_element = driver.find_element(By.CLASS_NAME, 'myselect')
    select_state = Select(state_element)
    state_options = select_state.options

    for state_option in state_options[1:]:
        state_text = state_option.text
        select_state.select_by_visible_text(state_text)
        time.sleep(2)

        district_element = driver.find_element(By.CLASS_NAME, 'myselect1')
        select_district = Select(district_element)
        district_options = select_district.options

        for district_option in district_options[1:]:
            district_text = district_option.text
            select_district.select_by_visible_text(district_text)
            time.sleep(2)

            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'lxml')
            table = soup.find("table")
            rows = table.find_all("tr")

            data_dicts = []
            for row in rows[1:]:
                cells = row.find_all("td")

                today_date = datetime.now().date()
                date = datetime.strptime(cells[0].text.strip(), "%Y-%m-%d").date()
                if date == today_date:
                    state = state_text
                    district = district_text
                    mandi = '0'
                    temperature = (int(cells[3].text.strip()) + int(cells[4].text.strip()))/2
                    humidity = (int(cells[5].text.strip()) + int(cells[6].text.strip()))/2
                    weather = '0'
                    source = f'{source_name}'

                    data_dict = weather_dictionary_data(state, district, mandi, temperature, humidity, weather, date, source)
                    data_dicts.append(data_dict)
            weather_insert_table(data_dicts)

    pass

################################################################################################################

def eNAM_mandi_code(row):
    print("eNAM_mandi code is running .....")

    mandi_create_table(row)
    driver = initialize_driver()
    driver.get(source_url)

    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'lxml')
    table = soup.find("table", class_=f"{source_table_class}")
    rows = table.find_all("tr")
    
    data_dicts = []
    for row in rows[1:]:
        cells = row.find_all("td")
        row = [tr.text for tr in cells]
        
        state = cells[1].text.strip()
        district = cells[2].text.strip()
        mandi = cells[3].text.strip()
        source = f'{source_name}'

        data_dict = mandi_dictionary_data(state, district, mandi, source)
        data_dicts.append(data_dict)
    
    mandi_insert_table(data_dicts)
    pass

################################################################################################################

source_query = "SELECT * FROM dim_source_testing;"
cursor.execute(source_query)
column_names = [column[0] for column in cursor.description]

for row in cursor.fetchall():
    row_dict = dict(zip(column_names, row))

    source_name = row_dict['source_name']
    table_name = row_dict['source_database_table']
    id_name = row_dict['source_id_name']
    source_url = row_dict['source_url']
    source_table_class = row_dict['source_table_class']

    if row_dict['source_type'] == 'commodity':
        if source_name == 'commodity_online': commodityonline_code(row_dict)
        elif source_name == 'eNAM': eNAM_code(row_dict)
    elif row_dict['source_type'] == 'weather':
        if source_name == 'mausam': mausam_code(row_dict)
    elif row_dict['source_type'] == 'mandi':
        if source_name == 'eNAM_mandi': eNAM_mandi_code(row_dict)


update_view()
conn.close()