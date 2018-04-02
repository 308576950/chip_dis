import pymysql
import datetime

conn=pymysql.connect(host='127.0.0.1',user='root',passwd='',db="pv_table", port=3306,charset='utf8')


#table_name_lists = []
cur=conn.cursor()
#cur.execute("show tables")
#tuple_databases = cur.fetchall()
#for item in tuple_databases:
#    table_name_lists.append(item[0])    # get all databases;

cur.execute("select TABLE_NAME from information_schema.tables where TABLE_SCHEMA='pv_table' and table_rows>0")
row = cur.fetchall()

#print(db_name_lists)
#print(len(table_name_lists))
count = 0
for item in row:
#    sql = "drop table %s"%(item[0])
#    cur.execute(sql)
#    print(sql)
#    row1 = cur.fetchall()
#    if row1[0] == (datetime.date(2015, 12, 31),):
#        count = count + 1
#print(count)

#    print(item[0])
#    if item[0][0] == 's':
#        if (item,) not in row:
        sql = "select Tra_Date from %s order by id desc limit 1"%(item[0])
        #sql = "use " + item
        cur.execute(sql)
        #sql = "show tables"
#        for iitem in list_tra_date:
#            sql = "delete from %s where Tra_Date='%s'"%(item , iitem[0].replace("-",""))

#        sql = "drop table " + item + ";"   # + " if exists " + item + ";" 
#        print(sql)
#        cur.execute(sql)
        #print(sql)
        row1 = cur.fetchall()
#        print(row1[0])
        if row1[0] == (datetime.date(2016, 1, 4),):
#        if row1[0] != "20151231":
            print(item, row1)
#            if row:
#                print(item, " duplicate in day ", row[0])
#            else:
#                print(item, " no duplicated records")
       
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
