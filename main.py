from xml.etree import ElementTree as ET
import pyodbc
from datetime import datetime, timedelta
import uuid
import os, logging
import shutil
import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler


conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=PRIORITYSQL\PRI;'
                      'Database=demo;'
                      'UID=tabula;'
                      'PWD=(game)T4bul4!')

cursor = conn.cursor()

def start_logging():
    path = r"error.log"
    logging.basicConfig(filename=path, level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info('ECO EPDM started.')

def get_pri_time():
    fmt = '%d/%m/%y'

    d1 = datetime.strptime('01/01/88', fmt)
    d2 = datetime.strptime(datetime.now().strftime('%d/%m/%y'), fmt)

    daysDiff = (d2-d1).days

    # Convert days to minutes
    minutesDiff = daysDiff * 24 * 60

    return minutesDiff

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

def parse_xml(path):
    myuuid = str(uuid.uuid4())
    # print(myuuid)

    doc = ET.parse(path).getroot()

    configuration = doc.find('./transactions/transaction/document/configuration')
    
    # get all attributes for the parent part
    attributes = configuration.findall('attribute')

    # print("---- Parent attributes ----")
    attributes = get_attributes(configuration)
    # print(attributes)

    # datetime(year=2017, month=3, day=1, hour=0, minute=0, second=1)
    max_line = get_max_line()
    # print(max_line)
    sql = '''INSERT INTO ZSFDC_LOADECO_M (LINE, BUBBLEID, RECORDTYPE, CURDATE, FROMDATE, DETAILS, OWNERLOGIN, ECOREASONCODE) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)'''
    val = (max_line + 1, myuuid, "1", get_pri_time(), get_pri_time(), attributes['details'], attributes['assigned_to'], attributes['eco_reason_code'])

    cursor.execute(sql, val)
    # conn.commit()

    max_line = get_max_line()
    sql = '''INSERT INTO ZSFDC_LOADECO_M (LINE, BUBBLEID, RECORDTYPE, CURDATE, FROMDATE, PARTNAME, PARTDES, PUNITNAME, UNITNAME, TYPE, CONV, FAMILYNAME) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
    val = (max_line + 1, myuuid, "2", get_pri_time(), get_pri_time(), attributes['part_name'][0:15], attributes['description'], attributes['buy_sell_unit'], attributes['factory_unit'], attributes['part_type'], attributes['conversion_ratio'], attributes['part_family'])
    cursor.execute(sql, val)
    # conn.commit()

    max_line = get_max_line()
    sql = '''INSERT INTO ZSFDC_LOADECO_M (LINE, BUBBLEID, RECORDTYPE, CURDATE, FROMDATE, FILEDATE, EXTFILENAME, NUMBER) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)'''
    val = (max_line + 1, myuuid, "3",get_pri_time(),get_pri_time(),get_pri_time(), attributes['pdf_location'], attributes['code'])
    cursor.execute(sql, val)
    # conn.commit()

    max_line = get_max_line()
    sql = '''INSERT INTO ZSFDC_LOADECO_M (LINE, BUBBLEID, RECORDTYPE, CURDATE, FROMDATE, REVNUM) 
            VALUES (?, ?, ?, ?, ?, ?)'''
    val = (max_line + 1, myuuid, "4",get_pri_time(),get_pri_time(), attributes['bom_revision'])
    cursor.execute(sql, val)
    # conn.commit()

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
            sql = '''INSERT INTO ZSFDC_LOADECO_M (LINE, BUBBLEID, RECORDTYPE, CURDATE, FROMDATE, SONNAME, SONREVNAME) 
                    VALUES (?, ?, ?, ?, ?, ?, ?)'''
            val = (max_line + 1, myuuid, "5",get_pri_time(),get_pri_time(), attributes['part_name'][0:15], attributes['bom_revision'])
            cursor.execute(sql, val)
            # conn.commit()  

    conn.commit()
    logging.info(f"File {path} parsed and loaded into table.")

def handle_files():
    start_logging()
    input_dir = os.fsencode('XML-Input')

    for file in os.listdir(input_dir):
        filename = os.fsdecode(file)
        if filename.endswith(".XML"):
            logging.info(f"Parsing file {filename}.")
            parse_xml(os.path.join('XML-Input', filename))
            path_to_current_file = os.path.join('XML-Input', filename)
            path_to_new_file = os.path.join('XML-Loaded', filename)
            shutil.move(path_to_current_file, path_to_new_file)

def run_load_proc():
    os.system('cmd /c "E:/Priority/bin.95/winrun "" tabula (game)T4bul4! E:\Priority\system\prep demo WINACTIV -P ZSFDC_LOADECO_M"')

def folder_event_handler():
    patterns = ["*"]
    ignore_patterns = None
    ignore_directories = False
    case_sensitive = True
    my_event_handler = PatternMatchingEventHandler(patterns, ignore_patterns, ignore_directories, case_sensitive)
    my_event_handler.on_created = on_created
    return my_event_handler

def folder_observer(my_event_handler):
    path = "./XML-Input"
    go_recursively = True
    my_observer = Observer()
    my_observer.schedule(my_event_handler, path, recursive=go_recursively)
    return my_observer


def on_created(event):
    print(f"Detected file: {event.src_path}.")
    handle_files()
    run_load_proc()

if __name__ == "__main__":
    my_observer = folder_observer(folder_event_handler())
    my_observer.start()
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        my_observer.stop()
        my_observer.join()
    # parse_xml("XML-Input/GB1-3910-15-00.XML")
    # handle_files()
    # run_load_proc()

#TODO: Error logging
# Create a procedure in priority that looks if the file exists in a given location and pass the filename
# one by one to the python program
# can an executable.

# below takes first argument
# import sys

# if __name__ == "__main__":
#     if len(sys.argv)>1:
#         print sys.argv[1]