from xml.etree import ElementTree as ET
import pyodbc
from datetime import datetime, timedelta
import uuid
import os
import shutil

# <ID>1</ID>
# <DESCRIPTION>Main leads, female for LTSL</DESCRIPTION>
# <MANUFACTURER>Eldon</MANUFACTURER>
# <PART_NUMBER>CLF2005L</PART_NUMBER>
# <INTERNAL_CODE>RMGE678</INTERNAL_CODE>
# <QUANTITY>1</QUANTITY>

def get_attributes(row):
    ID = row.find('ID').text
    DESCRIPTION = row.find('DESCRIPTION').text
    MANUFACTURER = row.find('MANUFACTURER').text
    PART_NUMBER = row.find('PART_NUMBER').text
    INTERNAL_CODE = row.find('INTERNAL_CODE').text
    QUANTITY = row.find('QUANTITY').text
    
    return {'ID': ID, 'DESCRIPTION': DESCRIPTION, 'MANUFACTURER': MANUFACTURER, 'PART_NUMBER': PART_NUMBER, 
            'INTERNAL_CODE': INTERNAL_CODE, 'QUANTITY': QUANTITY }    

def parse_xml(path):
    myuuid = str(uuid.uuid4())
    # print(myuuid)

    doc = ET.parse(path).getroot()

    row = doc.find('./Row')
    
    # get all attributes for the parent part
    # attributes = row.findall('row')

    # print("---- Parent attributes ----")
    # attributes = ge(t_attributes(row)

    
    print(get_attributes(row))
    
 

    
parse_xml(os.path.join('epdm.xml'))

    