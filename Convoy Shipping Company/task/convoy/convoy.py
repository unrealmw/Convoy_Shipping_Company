import json
import pandas as pd
import re
import csv
import sqlite3
from lxml import etree
import math


def csv_reader(csv_file):
    csv_lines = []
    with open(csv_file) as file:
        file_reader = csv.reader(file, delimiter=",")
        for line in file_reader:
            csv_lines.append(line)
    return csv_lines


def csv_writer(csv_lst, f_name):
    with open(f_name, "w", encoding='utf-8') as w_file:
        file_writer = csv.writer(w_file, delimiter=",", lineterminator="\n")
        for line in csv_lst:
            file_writer.writerow(line)


def csv_corrector(csv_list):
    csv_list_corrected = [csv_list[0]]
    to_correction = csv_list[1:]
    corrections = 0
    for lst in to_correction:
        for i in range(len(lst)):
            if not lst[i].isdigit():
                numbers = re.findall(r'\d+', lst[i])
                lst[i] = numbers[0]
                corrections += 1
    for lst in to_correction:
        csv_list_corrected.append(lst)
    return csv_list_corrected, corrections


def print_diff(num, name):
    if num == 1:
        print(f"{num} cell was corrected in {name}")
    elif num > 1:
        print(f"{num} cells were corrected {name}")
    else:
        pass


def to_sql(checked_filename):
    db_name = checked_filename.split("[")[0] + ".s3db"
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS convoy ("
                   "vehicle_id INT PRIMARY KEY,"
                   "engine_capacity INT NOT NULL,"
                   "fuel_consumption INT NOT NULL,"
                   "maximum_load INT NOT NULL,"
                   "score INT NOT NULL);")
    df = pd.read_csv(checked_filename)
    score = []
    for index, row in df.iterrows():
        term_1 = first_term(row["engine_capacity"], row["fuel_consumption"])
        term_2 = second_term(row["fuel_consumption"])
        term_3 = third_term(row["maximum_load"])
        score.append(sum([term_1, term_2, term_3]))
    df["score"] = score
    df.to_sql('convoy', conn, if_exists='append', index=False)
    cursor.execute('SELECT COUNT(*) FROM convoy')
    row_num = cursor.fetchone()[0]
    if row_num == 1:
        print(f"{row_num} record was inserted in {db_name}")
    else:
        print(f"{row_num} records were inserted into {db_name}")
    conn.commit()
    conn.close()
    return db_name


def first_term(tank_capacity, fuel_consumption):
    one_ride = tank_capacity / fuel_consumption * 100
    pit_stops = math.floor(450 / one_ride)
    if pit_stops >= 2:
        return 0
    elif pit_stops == 1:
        return 1
    elif pit_stops == 0:
        return 2


def second_term(fuel_consumption):
    total_fuel = 450 * fuel_consumption / 100
    if total_fuel <= 230:
        return 2
    else:
        return 1


def third_term(maximum_load):
    if maximum_load >= 20:
        return 2
    else:
        return 0


def data_from_sql(db_name, flag):
    name = db_name.split(".")[0]
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    if flag == "json":
        cursor.execute("SELECT vehicle_id, engine_capacity, fuel_consumption, maximum_load FROM convoy WHERE score > 3;")
    elif flag == "xml":
        cursor.execute("SELECT vehicle_id, engine_capacity, fuel_consumption, maximum_load FROM convoy WHERE score <= 3;")
    column_names = [description[0] for description in cursor.description]
    data = cursor.fetchall()
    conn.commit()
    conn.close()
    return column_names, data, name


def data_to_json(columns, data, name):
    json_name = name + ".json"
    json_data = []
    c = 0
    for inf in data:
        json_data.append(dict(zip(columns, inf)))
        c += 1
    json_file = {"convoy": json_data}
    with open(json_name, "w") as file:
        json.dump(json_file, file)
    if c == 1:
        print(f"{c} vehicle was saved into {json_name}")
    else:
        print(f"{c} vehicles were saved into {json_name}")


def data_to_xml(columns, data, name):
    xml_name = name + ".xml"
    c = 0
    xml_string = "<convoy>"
    if len(data) == 0:
        xml_string += "\n"
    for vehicle in data:
        c += 1
        xml_string += "<vehicle>"
        for column, param in zip(columns, vehicle):
            xml_string += f"<{column}>{param}</{column}>"
        xml_string += "</vehicle>"
    xml_string += "</convoy>"
    root = etree.fromstring(xml_string)
    tree = etree.ElementTree(root)
    tree.write(xml_name)
    if c == 1:
        print(f"{c} vehicle was saved into {xml_name}")
    else:
        print(f"{c} vehicles were saved into {xml_name}")


if __name__ == '__main__':
    infile_name = input("Input file name\n")
    if infile_name.endswith(".xlsx") or infile_name.endswith(".xls"):
        df = pd.read_excel(infile_name, sheet_name="Vehicles")
        file_name = infile_name.split(".")[0] + ".csv"
        lines = df.shape[0]
        if lines > 1:
            print(f"{lines} lines were added to {file_name}")
        else:
            print(f"{lines} line was added to {file_name}")
        df.to_csv(file_name, index=False, header=True)
        list_csv = csv_reader(file_name)
        cor_csv, cor = csv_corrector(list_csv)
        checked_file_name = infile_name.split(".")[0] + "[CHECKED].csv"
        print_diff(cor, checked_file_name)
        csv_writer(cor_csv, checked_file_name)
        database_name = to_sql(checked_filename=checked_file_name)
        json_columns, json_data, json_name = data_from_sql(database_name, "json")
        data_to_json(json_columns, json_data, json_name)
        xml_columns, xml_data, xml_name = data_from_sql(database_name, "xml")
        data_to_xml(xml_columns, xml_data, xml_name)
    elif infile_name.endswith(".csv") and "CHECKED" not in infile_name:
        list_csv = csv_reader(infile_name)
        cor_csv, cor = csv_corrector(list_csv)
        checked_file_name = infile_name.split(".")[0] + "[CHECKED].csv"
        print_diff(cor, checked_file_name)
        csv_writer(cor_csv, checked_file_name)
        database_name = to_sql(checked_filename=checked_file_name)
        json_columns, json_data, json_name = data_from_sql(database_name, "json")
        data_to_json(json_columns, json_data, json_name)
        xml_columns, xml_data, xml_name = data_from_sql(database_name, "xml")
        data_to_xml(xml_columns, xml_data, xml_name)
    elif infile_name.endswith("[CHECKED].csv"):
        database_name = to_sql(infile_name)
        json_columns, json_data, json_name = data_from_sql(database_name, "json")
        data_to_json(json_columns, json_data, json_name)
        xml_columns, xml_data, xml_name = data_from_sql(database_name, "xml")
        data_to_xml(xml_columns, xml_data, xml_name)
    elif infile_name.endswith(".s3db"):
        json_columns, json_data, json_name = data_from_sql(infile_name, "json")
        data_to_json(json_columns, json_data, json_name)
        xml_columns, xml_data, xml_name = data_from_sql(infile_name, "xml")
        data_to_xml(xml_columns, xml_data, xml_name)





