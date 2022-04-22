from xml.etree import ElementTree as ET
import pyodbc
import time    

conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=MEDA-LTP2620\PRI;'
                      'Database=demo;'
                      'UID=tabula;'
                      'PWD=<Pass>')

cursor = conn.cursor()
# cursor.execute('SELECT * FROM ORDERS')

# for i in cursor:
#     print(i)

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

doc = ET.parse('./XML/GB1b-5220-99-00.XML').getroot()

configuration = doc.find('./transactions/transaction/document/configuration')
   
# get all attributes for the parent part
attributes = configuration.findall('attribute')

print("---- Parent attributes ----")
attributes = get_attributes(configuration)

# sql = '''INSERT INTO ZSFDC_LOADECO_M (RECORDTYPE, CURDATE, FROMDATE, DETAILS, OWNERLOGIN, ECOREASONCODE) 
#           VALUES (%s, %s, %s, %s, %s, %s)'''
# val = ("1", time.strftime('%d/%m/%Y'), time.strftime('%d/%m/%Y'), attributes['details'], attributes['assigned_to'], attributes['eco_reason_code'])
# cursor.execute(sql, val)
# mydb.commit()

# sql = '''INSERT INTO ZSFDC_LOADECO_M (RECORDTYPE, CURDATE, FROMDATE, PARTNAME, PARTDES, PUNITNAME, UNITNAME, TYPE, CONV, FAMILYNAME) 
#           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
# val = ("2", time.strftime('%d/%m/%Y'), time.strftime('%d/%m/%Y'), attributes['part_name'], attributes['description'], attributes['buy_sell_unit'], attributes['factory_unit'], attributes['part_type'], attributes['conversion_ratio'], attributes['part_family'])
# cursor.execute(sql, val)
# mydb.commit()

# sql = '''INSERT INTO ZSFDC_LOADECO_M (RECORDTYPE, CURDATE, FROMDATE, FILEDATE, EXTFILENAME, NUMBER) 
#           VALUES (%s, %s, %s, %s, %s, %s)'''
# val = ("3", time.strftime('%d/%m/%Y'), time.strftime('%d/%m/%Y'), time.strftime('%d/%m/%Y'), attributes['pdf_location'], attributes['code'])
# cursor.execute(sql, val)
# mydb.commit()

# sql = '''INSERT INTO ZSFDC_LOADECO_M (RECORDTYPE, CURDATE, FROMDATE, REVNUM) 
#           VALUES (%s, %s, %s, %s)'''
# val = ("4", time.strftime('%d/%m/%Y'), time.strftime('%d/%m/%Y'), attributes['bom_revision'])
# cursor.execute(sql, val)
# mydb.commit()

# get first level child parts
references = configuration.find('references')

# Check if child exist before continuing
if references is not None:
    documents = references.findall('document')

    # get all attributes for the child part
    for document in documents:
        configuration = document.find('configuration')
        attributes = configuration.findall('attribute')
        print("---- Child attributes ----")                              
        attributes = get_attributes(configuration)
        
        # sql = '''INSERT INTO ZSFDC_LOADECO_M (RECORDTYPE, CURDATE, FROMDATE, SONNAME, SONREVNAME) 
        #           VALUES (%s, %s, %s, %s, %s)'''
        # val = ("5", time.strftime('%d/%m/%Y'), time.strftime('%d/%m/%Y'), attributes['part_name'], attributes['bom_revision'])
        # cursor.execute(sql, val)
        # mydb.commit()  
                                         