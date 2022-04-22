from xml.etree import ElementTree as ET
import pyodbc
import time
 
conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=PRIORITYSQL\PRI;'
                      'Database=gamec2;'
                      'UID=tabula;'
                      'PWD=15-small-head-phones')

cursor = conn.cursor()

def get_max_line():
    cursor.execute('SELECT MAX(LINE) FROM ZSFDC_LOADECO_M;')
    return cursor.fetchone()[0]

def get_attributes(config):
    part_name = config.find("*[@name='Number']").attrib['value']
    part_family = config.find("*[@name='Part Family']").attrib['value']
    part_type = config.find("*[@name='Part Type']").attrib['value']
    buy_sell_unit = config.find("*[@name='Buy / Sell Unit']").attrib['value']
    bom_revision = config.find("*[@name='Revision']").attrib['value']
    conversion_ratio = config.find("*[@name='Conversion Ratio']").attrib['value']
    assigned_to = config.find("*[@name='Assigned To']").attrib['value']
    description = config.find("*[@name='Description']").attrib['value']
    factory_unit = config.find("*[@name='Factory Unit']").attrib['value']
    pdf_location = config.find("*[@name='PDFLocation']").attrib['value']
    details = config.find("*[@name='ECO Details']").attrib['value']
    eco_reason_code = config.find("*[@name='ECO Reason']").attrib['value']
    code = config.find("*[@name='Code']").attrib['value']
    state = config.find("*[@name='State']").attrib['value']
    reference_count = config.find("*[@name='Reference Count']").attrib['value']      
    
    return {'part_name': part_name, 'part_family': part_family, 'part_type': part_type, 'buy_sell_unit': buy_sell_unit, 
            'bom_revision': bom_revision, 'conversion_ratio': conversion_ratio, 'assigned_to': assigned_to, 
            'description': description, 'factory_unit': factory_unit, 'pdf_location': pdf_location, 'details': details, 
            'eco_reason_code': eco_reason_code, 'code': code, 'state': state, 'reference_count': reference_count}                                                     

doc = ET.parse('./XML-Input/GB1b-5220-99-00.XML').getroot()

configuration = doc.find('./transactions/transaction/document/configuration')
   
# get all attributes for the parent part
attributes = configuration.findall('attribute')

# print("---- Parent attributes ----")
attributes = get_attributes(configuration)

# datetime(year=2017, month=3, day=1, hour=0, minute=0, second=1)
max_line = get_max_line()
sql = '''INSERT INTO ZSFDC_LOADECO_M (LINE, RECORDTYPE, CURDATE, FROMDATE, DETAILS, OWNERLOGIN, ECOREASONCODE) 
          VALUES (?, ?, ?, ?, ?, ?, ?)'''
val = (max_line, "1",int(time.time()), int(time.time()), attributes['details'], attributes['assigned_to'], attributes['eco_reason_code'])
cursor.execute(sql, val)
conn.commit()

max_line = get_max_line()
sql = '''INSERT INTO ZSFDC_LOADECO_M (LINE, RECORDTYPE, CURDATE, FROMDATE, PARTNAME, PARTDES, PUNITNAME, UNITNAME, TYPE, CONV, FAMILYNAME) 
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
val = (max_line, "2",int(time.time()),int(time.time()), attributes['part_name'], attributes['description'], attributes['buy_sell_unit'], attributes['factory_unit'], attributes['part_type'], attributes['conversion_ratio'], attributes['part_family'])
cursor.execute(sql, val)
conn.commit()

max_line = get_max_line()
sql = '''INSERT INTO ZSFDC_LOADECO_M (LINE, RECORDTYPE, CURDATE, FROMDATE, FILEDATE, EXTFILENAME, NUMBER) 
          VALUES (?, ?, ?, ?, ?, ?, ?)'''
val = (max_line, "3",int(time.time()),int(time.time()),int(time.time()), attributes['pdf_location'], attributes['code'])
cursor.execute(sql, val)
conn.commit()

max_line = get_max_line()
sql = '''INSERT INTO ZSFDC_LOADECO_M (LINE, RECORDTYPE, CURDATE, FROMDATE, REVNUM) 
          VALUES (?, ?, ?, ?, ?)'''
val = (max_line, "4",int(time.time()),int(time.time()), attributes['bom_revision'])
cursor.execute(sql, val)
conn.commit()

# get first level child parts
references = configuration.find('references')

# Check if child exist before continuing
if references is not None:
    documents = references.findall('document')

    # get all attributes for the child part
    for document in documents:
        configuration = document.find('configuration')
        attributes = configuration.findall('attribute')
        # print("---- Child attributes ----")                              
        attributes = get_attributes(configuration)
        
        max_line = get_max_line()
        sql = '''INSERT INTO ZSFDC_LOADECO_M (LINE, RECORDTYPE, CURDATE, FROMDATE, SONNAME, SONREVNAME) 
                  VALUES (?, ?, ?, ?, ?, ?)'''
        val = (max_line, "5",int(time.time()),int(time.time()), attributes['part_name'], attributes['bom_revision'])
        cursor.execute(sql, val)
        conn.commit()  
                                         