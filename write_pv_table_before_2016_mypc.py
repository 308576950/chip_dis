import pandas as pd
import os
import json
import pymysql
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor,as_completed
import time
import pdb
from warnings import filterwarnings
from DBUtils.PooledDB import PooledDB



filterwarnings('ignore', category=pymysql.Warning)

class OpeMysql(object):    # 每只股票都会有一个操作数据库的累
    def __init__(self, code):
        if code[0] == '6':
            self.table_name = 'sh' + code
        elif code[0] == '0' or code[0] == '3':
            self.table_name = 'sz' + code
        self.create(self.table_name)

    def create(self, table_name):    # 专门创建某只股票的数据库
        conn = pymysql.connect(host='127.0.0.1', user='root', passwd='', port=3306,
                               db='pv_table', charset='utf8')
        #conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,db='pv_table', charset='utf8')
        cur = conn.cursor()
        sql_cre_table = "create table if not exists %s(id int auto_increment primary key,Tra_Date date not null,Chip mediumtext not null, pre_p double, sup_p double)" % table_name
        cur.execute(sql_cre_table)     # 创建分价表table
        conn.commit()
        cur.close()
        conn.close()


    def destroy(self, table_name):
        conn = pymysql.connect(host='127.0.0.1', user='root', passwd='', port=3306,
                               db='pv_table', charset='utf8')
        #conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,db='pv_table', charset='utf8')
        cur = conn.cursor()
        sql = "drop table if exists " + table_name
        cur.execute(sql)  # 删除数据库
        conn.commit()
        cur.close()
        conn.close()

    def insert(self, date, record):    # 将某天的分价表插入到数据库当中
        conn = pymysql.connect(host='127.0.0.1', user='root', passwd='', port=3306,
                               db='pv_table', charset='utf8')
        #conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,db='pv_table', charset='utf8')
        cur = conn.cursor()
        # sql_count = "select count(*) from %s where Tra_Date=%s"%(self.table_name, date)
        #
        # if

        sql_insert_record = "insert into %s (Tra_Date,Chip, pre_p, sup_p) values('%s','%s',0.0, 0.0)" % (self.table_name, date, record)
        cur.execute(sql_insert_record)
        conn.commit()
        cur.close()
        conn.close()

    def insert_records(self, df):    # 一次性插入所有pv_table
        conn = pymysql.connect(host='127.0.0.1', user='root', passwd='', port=3306,
                               db='pv_table', charset='utf8')
        #conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,db='pv_table', charset='utf8')
        cur = conn.cursor()

        for index, row in df.iterrows():
            sql_insert_record = "insert into %s (Tra_Date,Chip, pre_p, sup_p) values('%s','%s',0.0, 0.0)" % (self.table_name, str(index), row["pv_table"])
            cur.execute(sql_insert_record)
        conn.commit()
        cur.close()
        conn.close()

    def get(self, date):     # 读取某天的分价表
        conn = pymysql.connect(host='127.0.0.1', user='root', passwd='', port=3306,
                               db='pv_table', charset='utf8')
        #conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,db='pv_table', charset='utf8')
        cur = conn.cursor()
        sql_select_record = "select * from %s where Tra_Date ='%s'" % (self.table_name, date)
        cur.execute(sql_select_record)
        conn.commit()
        cur.close()
        row = cur.fetchall()
        conn.close()
        if len(row):
            return eval(row[0][2])
        else:
            return ""

#     def write_pv_table(code):   # 函数里产生类，函数用于多线程
#         test = GetPVTable(code)
#         test.write_mysql()


def write_mysql(iitem):

#    conn=pymysql.connect(host='127.0.0.1',user='root',passwd='',db="pv_table", port=3306,charset='utf8')
    #conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,db='pv_table', charset='utf8')
#    table_name_lists = []
#    cur=conn.cursor()
#    cur.execute("show tables")
    
    # 有些为空，重新写进去
#    cur.execute("select TABLE_NAME from information_schema.tables where TABLE_SCHEMA='pv_table' and table_rows>0")


#    tuple_databases = cur.fetchall()
#    for item in tuple_databases:
#        table_name_lists.append(item[0])    # get all databases;
     
#    pdb.set_trace()    

#    result = {}
#     yesterday = list(df.index)[list(df.index).index(19981210) - 1]
#    if iitem[2] == "6":
#        name = iitem[2:] + ".SH.CSV"
#    else:
#        name = iitem[2:] + ".SZ.CSV"

#    pdb.set_trace()
#    if iitem not in table_name_lists:
    df = pd.read_csv("/data/stocks_close_price/stocks_close_price/" + iitem, encoding='gbk', usecols=[0, 1, 2, 3, 4],index_col=2)
    close_price = df["收盘价(元)"]
    df["收盘价(元)"] = close_price.round(2)
    df["pv_table"] = ""
    tmp_dict = {}
    price_time = {}
    

#        ope_mysql = OpeMysql(iitem[2:])
    records = []
    for index, row in df.iterrows():
        # try:
        #     tmp_dict =        
        #ope_mysql.get(str(yesterday))   # 如果中断了 则重新算  需要用到前一天的筹码
        # except Exception as e:
        #     print(str(index) + "data not exists")
        #     tmp_dict = {}
        # if not len(ope_mysql.get(str(index))):   #ru guo bu cun zai
        if row["换手率(%)"] != "--":  # 未停牌
            if row["收盘价(元)"] in tmp_dict.keys():  # 已经存在的价格
                tmp_dict[row["收盘价(元)"]] = tmp_dict[row["收盘价(元)"]] + float(row["换手率(%)"])
            else:
                tmp_dict[row["收盘价(元)"]] = float(row["换手率(%)"])

            price_time[row["收盘价(元)"]] = index

            pv_table = {}
            count = 0
            for key, value in tmp_dict.items():
                if count <= 500:
                    if index - price_time[key] < 20000:  # 两年内的价格才计算进去
                        pv_table[key] = value
                        count = count + 1
            sum_value = sum(pv_table.values())
            for key, value in pv_table.items():
                pv_table[key] = value/sum_value
                       
            # code tra_date chip
            #pdb.set_trace()
            records.append((iitem[0:6], str(index), str(pv_table)))
    #pool = PooledDB(pymysql,50,host='127.0.0.1',user='root',passwd='',db="pv_table",port=3306, charset="utf8")
    conn=pymysql.connect(host='127.0.0.1',user='root',passwd='',db="pv_table", port=3306,charset='utf8')
    #conn = pool.connection()    
    cur = conn.cursor()
    try:        
        if iitem[0] == '6':
            cur.executemany("insert into pricetable_zb (code, tra_date, chip) values(%s,%s,%s)", records)
        if iitem[0] == '0':
            cur.executemany("insert into pricetable_zxb (code, tra_date, chip) values(%s,%s,%s)", records)
        if iitem[0] == '3':
            cur.executemany("insert into pricetable_cyb (code, tra_date, chip) values(%s,%s,%s)", records)
        
        conn.commit()
        print(iitem[0:6], " over")
    except Exception as e:
        conn.rollback()
        print("Exception: ", str(e))     


if __name__ == '__main__':
    t1 = time.time()
    files_name = []

    for (root, dirs, files) in os.walk("/data/stocks_close_price/stocks_close_price"):     #E:\wind_export_files\stocks_close_price
        files_name.append(files)

    print(len(files_name[0]))

    #conn=pymysql.connect(host='127.0.0.1',user='root',passwd='',db="pv_table", port=3306,charset='utf8')
#    conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,db='pv_table', charset='utf8')
#    table_name_lists = []
#    cur=conn.cursor()
#    cur.execute("show tables")

    # 有些为空，重新写进去
#    cur.execute("select TABLE_NAME from information_schema.tables where TABLE_SCHEMA='pv_table' and table_rows=0")


#    tuple_databases = cur.fetchall()
#    for item in tuple_databases:
#        table_name_lists.append(item[0])    # get 空 databases;
#    table_name_lists = ['sz002501', 'sz300206' ,'sz000415', 'sz002461']

#    for iitem in files_name[0]:
#        future = write_mysql(iitem)



#    result = {}
#     yesterday = list(df.index)[list(df.index).index(19981210) - 1]
#    if iitem[0] == "6":
#        name = "sh" + iitem[0:6]
#    else:
#        name = "sz" + iitem[0:6]
#    write_mysql(last_one)
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@2
#   futures = set()
    with ProcessPoolExecutor(4) as executor:
        for iitem in files_name[0]:
            executor.submit(write_mysql, iitem)
#            future = write_mysql(iitem)
#            futures.add(future)
#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

#        for iitem in list(set(sum_df["SecurityID"])):  # 代码集合
#            # write_oneday_pricetable(iitem, row_list_tables, date, sum_df, initial_info)
#            future = executor.submit(write_oneday_pricetable, iitem, row_list_tables, date, sum_df, initial_info)
#            futures.add(future)
#    try:
#        for future in as_completed(futures):
#            err = future.exception()
#            if err is not None:
#                raise err
#    except KeyboardInterrupt:
#            print("stopped by hand")



 
#    tables_name_lists = ['600653.SH.CSV'] 
#    for iitem in table_name_lists:
#        write_mysql(iitem)
#        print(iitem)

#    with ProcessPoolExecutor(4) as executor:
#        for iitem in files_name[0]:
#            if iitem[0] == "6":
#                name = "sh" + iitem[0:6]
#            else:
#                name = "sz" + iitem[0:6]
#            if name not in table_name_lists:           
#                executor.submit(write_mysql, iitem)
#            else:
#                print(iitem," exists")
    t2 = time.time()
    print(t2 - t1)


#
# for iitem in files_name[0][1:]:
#     pass



