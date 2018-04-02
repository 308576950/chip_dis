import pymysql
import urllib.request
import json
import pandas as pd
from DBUtils.PooledDB import PooledDB
import pdb
import datetime

def create_pricetable():
    #conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,
    #                       db='pv_table', charset='utf8')
    conn=pymysql.connect(host='127.0.0.1',user='root',passwd='',db="pv_table", port=3306,charset='utf8')
    cur = conn.cursor()
    try:
        sql_create_table_zb = '''
        CREATE TABLE if not exists pricetable_zb(
                        code char(6) not null,
                        tra_date date not null,
                        chip mediumtext,
                        pre_p float,
                        sup_p float,
                        close float,
                        pre_effort float,
                        sup_effort float,
                        primary key (code,tra_date)
        );
        '''
        cur.execute(sql_create_table_zb)
    except Exception as e:
        conn.rollback()
        print("Exception: ", str(e))

    try:
        sql_create_table_zxb = '''
        CREATE TABLE if not exists pricetable_zxb(
                        code char(6) not null,
                        tra_date date not null,
                        chip mediumtext,
                        pre_p float,
                        sup_p float,
                        close float,
                        pre_effort float,
                        sup_effort float,
                        primary key (code,tra_date)
        );
        '''
        cur.execute(sql_create_table_zxb)
    except Exception as e:
        conn.rollback()
        print("Exception: ", str(e))

    try:
        sql_create_table_cyb = '''
        CREATE TABLE if not exists pricetable_cyb(
                        code char(6) not null,
                        tra_date date not null,
                        chip mediumtext,
                        pre_p float,
                        sup_p float,
                        close float,
                        pre_effort float,
                        sup_effort float,
                        primary key (code,tra_date)
        );
        '''
        cur.execute(sql_create_table_cyb)
    except Exception as e:
        conn.rollback()
        print("Exception: ", str(e))
    print("Three tables created over")


def transfer(table_name, row):
#    conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,
#                           db='pv_table', charset='utf8')
#    #conn=pymysql.connect(host='127.0.0.1',user='root',passwd='',db="pv_table", port=3306,charset='utf8')
#    #cur = conn.cursor()
#
#    sql = "select Tra_Date, Chip from %s"%(table_name)
#    cur.execute(sql)
#    row = cur.fetchall()

    if table_name[2] == '6':
        tmp_pricetable = "pricetable_zb"
    if table_name[2] == '0':
        tmp_pricetable = "pricetable_zxb"
    if table_name[2] == '3':
        tmp_pricetable = "pricetable_cyb"

    records = []
    for item in row:
        if item[0] != datetime.date(2016,1,4):
        #sql = "insert into %s (code, tra_date, chip) values('%s','%s','%s')" % (tmp_pricetable, table_name[2:], item[0], item[1])
            records.append((table_name[2:], item[0], item[1]))
    
    pool = PooledDB(pymysql,50,host='127.0.0.1',user='root',passwd='',db="pv_table",port=3306, charset="utf8")
    conn = pool.connection()

    #conn=pymysql.connect(host='127.0.0.1',user='root',passwd='',db="pv_table", port=3306,charset='utf8')
    cur=conn.cursor()
    try:
        if tmp_pricetable == "pricetable_zb":
            cur.executemany("insert into pricetable_zb (code, tra_date, chip) values(%s,%s,%s)", records)
        if tmp_pricetable == "pricetable_zxb":
            cur.executemany("insert into pricetable_zxb (code, tra_date, chip) values(%s,%s,%s)", records)
        if tmp_pricetable == "pricetable_cyb":
            cur.executemany("insert into pricetable_cyb (code, tra_date, chip) values(%s,%s,%s)", records)
        #cur.executemany("insert into %s (code, tra_date, chip) values('%s','%s','%s')", records)
        conn.commit()
        print(table_name, " over")
        #cur.close()
        #conn.close()
    except  Exception as e:
        conn.rollback()
        print(table_name[2:], "Exception: ", str(e))

#  复权处理。复权处理复权处理复权处理复权处理复权处理 ￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥￥


if __name__ == '__main__':
    # url_tra_date = "http://fintech.jrj.com.cn/tp/astock/getholidays?start=1990-12-01&end=2018-01-01"
    # res_tra_date = urllib.request.urlopen(url_tra_date)
    # html_tra_date = res_tra_date.read()
    # list_tra_date = json.loads(html_tra_date.decode('utf-8'))['data']
    #
    # initial_info = pd.read_csv("./initial_info.csv", encoding='gbk', index_col=0)   # 获得所有股票的ipo时间

    #create_pricetable()

    #conn = pymysql.connect(host='127.0.0.1', user='root', passwd='', db="pv_table", port=3306, charset='utf8')
    conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,
                           db='pv_table', charset='utf8')

    table_name_lists = []
    cur = conn.cursor()
    cur.execute("show tables")
    tuple_databases = cur.fetchall()
    for item in tuple_databases:
        table_name_lists.append(item[0])  # get all databases;
    #pdb.set_trace()
    # print(db_name_lists)
    #print(len(table_name_lists))
    for item in table_name_lists[10:]:
        if item[0] == "s":
            sql = "select Tra_Date, Chip from %s"%(item)
            cur.execute(sql)
            row = cur.fetchall()
            transfer(item, row)
