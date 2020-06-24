#!/usr/bin/env  python3
import pprint
import sqlite3
import argparse
import xlsxwriter
import mysql.connector

parser = argparse.ArgumentParser(description='Process db file')
parser.add_argument('--filename', help='sqlite3 filename')
parser.add_argument("--export", help="Export all tables to xlsx", action="store_true")
args = parser.parse_args()


def read_data_sqlite3(filename):
    conn_lite = sqlite3.connect(filename)
    cursor_lite = conn_lite.cursor()
    cursor_lite.execute("SELECT * FROM univer")
    return cursor_lite.fetchall()



def write_data_mysql(data):
    conn_mysql = mysql.connector.connect(
            host='localhost',
            user='temp',
            password='123',
            database='univer'
        )
    cursor_mysql = conn_mysql.cursor()
    cursor_mysql.execute('drop table if exists cars')
    cursor_mysql.execute('drop table if exists engines')
    cursor_mysql.execute('drop table if exists transmissions')
    cursor_mysql.execute('drop table if exists brands')
    cursor_mysql.execute('drop table if exists wheels')
    #create tables
    cursor_mysql.execute('create table cars(model varchar(255), brand varchar(255),engine varchar(255), transmission varchar(255), price integer(255),wheel varchar(255))')
    cursor_mysql.execute('create table engines(engine_model varchar(255),engine_power integer(255),engine_volume integer(255), engine_type varchar(255))')
    cursor_mysql.execute('create table transmissions(transmission_model varchar(255),transmission_type varchar(255),transmission_gears_number integer(255))')
    cursor_mysql.execute('create table brands(brand_name varchar(255), brand_creator_country varchar(255))')
    cursor_mysql.execute('create table wheels(wheel_model varchar(255), wheel_radius integer(255), wheel_color varchar(255))')
    #add primary keys
    cursor_mysql.execute('alter table cars add primary key(model)')
    cursor_mysql.execute('alter table engines add primary key(engine_model)')
    cursor_mysql.execute('alter table transmissions add primary key(transmission_model)')
    cursor_mysql.execute('alter table brands add primary key(brand_name)')
    cursor_mysql.execute('alter table wheels add primary key(wheel_model)')
    #add connections
    for item in data:
        #print(item)
        #('2114', 'LADA', 'Russia', 'V123', '80', '16', 'L4', 'M123', 'M', '5', 'Best kolesa', '13', 'White', '100000')
        cursor_mysql.execute(f'INSERT INTO cars(model,brand,engine,transmission,price,wheel) values ("{item[0]}","{item[1]}","{item[3]}","{item[7]}",{item[13]},"{item[10]}") ON DUPLICATE KEY UPDATE model = "{item[0]}"')
        cursor_mysql.execute(f'INSERT INTO engines(engine_model,engine_power,engine_volume,engine_type) values ("{item[3]}",{item[4]},{item[5]},"{item[6]}") ON DUPLICATE KEY UPDATE engine_model = "{item[3]}"')
        cursor_mysql.execute(f'INSERT INTO transmissions(transmission_model,transmission_type,transmission_gears_number) values ("{item[7]}","{item[8]}",{item[9]}) ON DUPLICATE KEY UPDATE transmission_model = "{item[7]}"')
        cursor_mysql.execute(f'INSERT INTO brands(brand_name,brand_creator_country) values ("{item[1]}","{item[2]}") ON DUPLICATE KEY UPDATE brand_name = "{item[1]}"') 
        cursor_mysql.execute(f'INSERT INTO wheels(wheel_model,wheel_radius,wheel_color) values ("{item[10]}",{item[11]},"{item[12]}") ON DUPLICATE KEY UPDATE wheel_model = "{item[10]}"') 
    cursor_mysql.execute('alter table cars add constraint cars_brand_fk foreign key (brand) references brands (brand_name)')
    cursor_mysql.execute('alter table cars add constraint cars_engine_fk foreign key (engine) references engines (engine_model)')
    cursor_mysql.execute('alter table cars add constraint cars_transmission_fk foreign key (transmission) references transmissions (transmission_model)')
    cursor_mysql.execute('alter table cars add constraint cars_wheel_fk foreign key (wheel) references wheels (wheel_model)')
    conn_mysql.commit()

    conn_mysql.close()
    print(f"Successfully inserted to univer ")
    return "kek"


def export():
    #create connection to mysql
    conn_mysql = mysql.connector.connect(
            host='localhost',
            user='temp',
            password='123',
            database='univer'
        )
    cursor_mysql = conn_mysql.cursor()
    #get list of tables
    cursor_mysql.execute("SHOW tables")
    tables = cursor_mysql.fetchall()
    #create xlsx file
    workbook = xlsxwriter.Workbook('all_tables.xlsx')
    worksheet = workbook.add_worksheet('MENU')
    #create style for xlsx file
    header_cell_format = workbook.add_format({'bold': True, 'border': True, 'bg_color': 'yellow'})
    body_cell_format = workbook.add_format({'border': True})
    row_index = 0
    for tablename_raw in tables:
        rows = []
        tablename = str(tablename_raw)[2:-3]
        print(tablename)
        cursor_mysql.execute(f'select * from {tablename}')
        header = [row[0] for row in cursor_mysql.description]
        rows += cursor_mysql.fetchall()


        column_index = 0
        row_index += 1
        worksheet.write(row_index, column_index, tablename, header_cell_format)
        row_index += 1

        for column_name in header:
            worksheet.write(row_index, column_index, column_name, header_cell_format)
            column_index += 1

        row_index += 1
        for row in rows:
            column_index = 0
            for column in row:
                # write
                worksheet.write(row_index, column_index, column, body_cell_format)
                column_index += 1
            row_index += 1

    print(str(row_index) + ' rows written successfully to ' + workbook.filename)
    workbook.close()
    conn_mysql.close()




if __name__=="__main__":
    if args.export and args.filename:
        data = read_data_sqlite3(args.filename)
        write_data_mysql(data)
        export()
    elif args.filename:
        data = read_data_sqlite3(args.filename)
        write_data_mysql(data)
    elif args.export:
        export()
    else:
        print("Add some functions or --help for help")










