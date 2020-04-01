from mySQLConnection import *

def createMySQLTables():
    # sql = "set global max_allowed_packet=" + max_allowed_packet
    # cur.execute(sql)  

    sql = "CREATE TABLE IF NOT EXISTS raw_data(\
        date_time timestamp(3) not null,\
        raw_data json not null,\
        machine_num varchar(10) not null,\
        line varchar(10) not null,\
        floor varchar(10) not null,\
        location varchar(10) not null,\
        primary key(date_time,machine_num,floor,line,location));"
    cur.execute(sql)    

    sql = "CREATE TABLE IF NOT EXISTS basic_analysis_data(\
        date_time timestamp(3) not null,\
        raw_data json not null,\
        flag_working boolean not null,\
        flag_change_part boolean not null,\
        uuid varchar(50),\
        machine_num varchar(10) not null,\
        line varchar(10) not null,\
        floor varchar(10) not null,\
        location varchar(10) not null,\
        primary key (date_time, machine_num,floor,line,location));"
    cur.execute(sql)    
        
    sql = "CREATE TABLE IF NOT EXISTS column_meta(\
        id int auto_increment not null,\
        column_name varchar(50) not null,\
        value varchar(10)not null,\
        last_date timestamp(3) not null,\
        machine_num varchar(10) not null,\
        line varchar(10) not null,\
        floor varchar(10)not null,\
        location varchar(10) not null,\
        key(id),primary key (column_name, machine_num,floor,line,location));"
    cur.execute(sql)    