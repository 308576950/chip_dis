import numpy as np
from heapq import nlargest
import math
import pymysql
import pandas as pd
import multiprocessing
import pdb
from DB_connetion_pool import getPTConnection, PTConnectionPool;

def extreme(my_dict, close):   # 思路是在收盘价的一个涨跌幅之内最大的筹码密集区
    # 在区间内做平滑处理
    s_p_price = {}
    if len(my_dict) < 5:  # 价格小于5个则认为没有支撑位和压力位
        s_p_price['S'] = 0
        s_p_price['P'] = 0
        print("价格数目太少，没有支撑压力位")
        return s_p_price
    else:
        # average_n = math.floor(len(my_dict) / 5)    移动平均，效果并不好，下一步来测试画出包络图
        # chip_list = list(my_dict.values())
        # tmp = []
        # for i in range(average_n, len(chip_list) - average_n):
        #     tmp.append(np.array(chip_list[i - average_n:i + average_n]).mean())
        # tmp = chip_list[:average_n] + tmp + chip_list[average_n:]
        # ma_my_dict = dict(zip(list(my_dict.keys()), tmp))

        # dict_list = []
        # for k, v in my_dict.items():
        #     dict_list.append({k: v})   # 为了利用nlargest函数，将dict变成了[dict]的格式
        # envelope = nlargest(8, dict_list, key=lambda s: s["chip"])   # 取最大的8个作为包络

        my_array1 = np.arange(round(0.9 * close, 2), close-0.01, 0.01)   # 低于收盘价，计算支撑位
        my_array2 = np.arange(close, round(1.1 * close, 2), 0.01)    # 高于收盘价，计算压力位

        tmp1 = []
        tmp2 = []

        for i in my_array1:
            if round(i, 2) in list(my_dict.keys()):
                tmp1.append({"price": i, "chip": my_dict[round(i, 2)]})     # 找出支撑位的筹码备选区间
        if nlargest(1, tmp1, key=lambda s: s["chip"]):
            s_p_price['S'] = round(nlargest(1, tmp1, key=lambda s: s["chip"])[0]["price"], 2)
        else:
            s_p_price['S'] = 0

        for i in my_array2:             # 找出压力位的筹码备选区间
            if round(i, 2) in list(my_dict.keys()):
                tmp2.append({"price": i, "chip": my_dict[round(i, 2)]})
        if nlargest(1, tmp2, key=lambda s: s["chip"]):
            s_p_price['P'] = round(nlargest(1, tmp2, key=lambda s: s["chip"])[0]["price"], 2)
        else:
            s_p_price['P'] = 0
        return s_p_price       #  {"S":支撑位, "P":压力位}


# 上面写了由筹码表计算支撑压力位的函数，下一步直接用之前写的类读取筹码分布表


def cal_one_stock_sp_price(code):
    # 计算一只股票的所有的SP价格  思路  先读取所有非空的chip和close组合。然后整体计算汇成list，最后再executemany
    
    code_table = {'6':"pricetable_zb", '0':"pricetable_zxb", '3':"pricetable_cyb"}
    pricetable = code_table[code[0]]
    


    sql_get_all_records = "select code, tra_date, chip, close  from %s where length(chip)>4 and length(close)>=4 "%(table_name)  # 找出所有的chip和close不为空的记录
    #conn = pymysql.connect(host='127.0.0.1', user='root', passwd='', db="pv_table", port=3306, charset='utf8')

    #conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,
    #                       db='pv_table', charset='utf8')

    cur = conn.cursor()

    # 获取所有记录，一次性算完之后再写回到Mysql
    cur.execute(sql_get_all_records)
    records_tuple = cur.fetchall()

    records=[]

    for item in records_tuple:
        tmp_pv_table = eval(item[2])
        close = item[4]    # id date chip p_price c_rice close
        pv_table = {}

        for key, value in tmp_pv_table.items():
            pv_table[float(key)] = value

        sp_price_dict = extreme(pv_table, close)    # 需要获得前复权价格   002668 NoneType has no attribute 'item'

        records.append((item[0], item[1],sp_price_dict["P"], sp_price_dict["S"]))
# 所有记录形成list

    with getPTConnection() as db:
        try:
            if table_name == "pricetable_zb":
                #db.cursor.executemany("insert into pricetable_zb (code, tra_date, close) values(%s,%s,%f)", records)
                db.cursor.executemany("update pricetable_zb set pre_p=%s. sup_p=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))

            if table_name == "pricetable_zxb":
                #db.cursor.executemany("insert into pricetable_zxb (code, tra_date, close) values(%s,%s,%f)", records)
                db.cursor.executemany("update pricetable_zxb set close=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
            if table_name == "pricetable_cyb":
                #db.cursor.executemany("insert into pricetable_cyb (code, tra_date, close) values(%s,%s,%f)", records)
                db.cursor.executemany("update pricetable_cyb set close=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
            db.conn.commit()
            print(code, " over")
        except Exception as e:
            print("Exception: ", str(e))
            db.conn.rollback()





        sql_insert_ps = "insert into %s (s_price, p_price) values(%s, %s)"%(sp_price_dict["S"], sp_price_dict["P"])

        try:
            cur.execute(sql_insert_ps)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print("Exception: ", str(e))


def insert_close(table_name, code, conn):   # 增加close_price字段
    # 思路  一次读取完所有的价格 然后executemany
    cur = conn.cursor()
    records = []
    sql_get_tradates = "select tra_date from %s where code = '%s'" % (table_name, code)
    cur.execute(sql_get_tradates)   # 获得所有交易日
    row_date_list = cur.fetchall()

    file_name = {'6':'.SH.CSV', '0':".SZ.CSV", '3':".SZ.CSV"}    

    df = pd.read_csv("/data/write_mysql_20180325/re_write/front_exclude_close/" + code + file_name[code[0]], index_col=2, encoding="gbk")

    for item in row_date_list:
        try:
            close_price = df.loc[int(str(item[0]).replace('-','')),'收盘价(元)'] 
        except Exception as e:
            print("Exception: ", str(e))
            close_price = 0.0
        records.append((float(close_price), code, str(item[0]).replace('-','')))   # 读取记录，存入tuple中
 
    with getPTConnection() as db:
        try:
            if table_name == "pricetable_zb":
                #db.cursor.executemany("insert into pricetable_zb (code, tra_date, close) values(%s,%s,%f)", records)
                db.cursor.executemany("update pricetable_zb set close=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))

            if table_name == "pricetable_zxb":
                #db.cursor.executemany("insert into pricetable_zxb (code, tra_date, close) values(%s,%s,%f)", records)
                db.cursor.executemany("update pricetable_zxb set close=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
            if table_name == "pricetable_cyb":
                #db.cursor.executemany("insert into pricetable_cyb (code, tra_date, close) values(%s,%s,%f)", records)
                db.cursor.executemany("update pricetable_cyb set close=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
            db.conn.commit()
            print(code, " over")
        except Exception as e:
            print("Exception: ", str(e))
            db.conn.rollback()


def cal_one_table_sp_price(table):
     sql_get_tables_from_table = "select distinct code from %s"%table
     cur.execute(sql_get_tables_from_table)
     row_list_codes = cur.fetchall()
     for item in row_list_codes:
            #pool.apply_async(insert_close, (table, item[0], conn))
         cal_one_stock_sp_price(item[0])  # 计算支撑压力位,支撑压力位的计算需要用到close


if __name__ == '__main__':
    # 第一步，读取pv_table库中table列表，既得股票名称列表
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table", port=3306, charset='utf8')
    #conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,
    #                       db='pv_table', charset='utf8')

    # select code, tra_date from pricetable_zb where code = '603999';
    # select distinct code from pricetable_zb;

    cur = conn.cursor()
    #sql_get_all_tables = "select table_name from information_schema.TABLES where TABLE_SCHEMA='pv_table'"
    #cur.execute(sql_get_all_tables)
        ## 类似格式 (("sh600000",),("sz000001", ))

    code_table = ["pricetable_zb", "pricetable_zxb", "pricetable_cyb"]

    #pool = multiprocessing.Pool(processes=4)


#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$   写入收盘价
#    for table in code_table:
#        sql_get_tables_from_table = "select distinct code from %s"%table
#        cur.execute(sql_get_tables_from_table)
#        row_list_codes = cur.fetchall()
#        for item in row_list_codes:
#            #pool.apply_async(insert_close, (table, item[0], conn))
#            insert_close(table, item[0], conn)               # 增加close字段
#
#        #pool.close()
#        #pool.join()
#
#        print(table, "over")

#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    for table in code_table:
        cal_one_table_sp_price(table)        


#    for item in row_list_tables:
#         cal_one_stock_sp_price(item[0])  # 计算支撑压力位,支撑压力位的计算需要用到close


    # futures = set()
    # with ProcessPoolExecutor(4) as executor:
    #     for item in row_list_tables:
    #         table_name = item[0]
    #         # write_oneday_pricetable(iitem, row_list_tables, date, sum_df, initial_info)
    #         future = executor.submit(cal_one_stock_sp_price, table_name)
    #         futures.add(future)
    #
