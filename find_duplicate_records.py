from sqlalchemy import create_engine
import pymysql

conn=pymysql.connect(host='127.0.0.1',user='root',passwd='',db="pv_table", port=3306,charset='utf8')


table_name_lists = []
cur=conn.cursor()
cur.execute("show tables")
tuple_databases = cur.fetchall()
for item in tuple_databases:
    table_name_lists.append(item[0])    # get all databases;

#print(db_name_lists)
#print(len(table_name_lists))
for item in table_name_lists:
    if item[0] == 's':
        #sql = "use " + item
        #cur.execute(sql)
        #sql = "show tables"

        sql = "select Tra_Date,count(*) as count from %s group by Tra_Date having count>1"%item

#        sql = "drop table " + item + ";"   # + " if exists " + item + ";" 
#        print(sql)
        cur.execute(sql)
        row = cur.fetchall()
        if row:
            print(item, " duplicate in day ", row[0])
        else:
            print(item, " no duplicated records")
       
'''
for item in db_name_lists:
    if item[0] == 's':
        sql = "use " + item
        cur.execute(sql)
        sql = "show tables"
        tuple_databases = cur.fetchall()
        for iitem in tuple_databases:
            print(iitem[0])

#print(db_name_lists)

sql = """CREATE TABLE chip (
         Ddate  DATE NOT NULL,
         CHIP ,
         AGE INT,  
         SEX CHAR(1),
         INCOME FLOAT )"""

drop_name = "test"
sql = "drop database " + drop_name
cur.execute(sql)
'''
