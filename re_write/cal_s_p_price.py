import numpy as np
from heapq import nlargest
#import math
import pymysql
import pandas as pd
import multiprocessing
import pdb
from DB_connetion_pool import getPTConnection, PTConnectionPool;

def extreme(my_dict, close):   # 思路是在收盘价的一个涨跌幅之内最大的筹码密集区
    # 在区间内做平滑处理
    s_p_price = {}
    if len(my_dict) < 1:  # 价格小于5个则认为没有支撑位和压力位
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

        my_array1 = np.arange(round(0.9 * close, 2), close, 0.01)   # 低于收盘价，计算支撑位
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
    table_name = code_table[code[0]]
    
#    sql_get_all_records = "select code, tra_date, chip, close  from %s where code=%s"%(table_name, code)  # 找出所有的chip和close不为空的记录
#    sql_get_all_records = "select code, tra_date, chip, close  from %s where code=%s and tra_date<'20160101'"%(table_name, code)  # 找出所有的chip和close不为空的记录
#    sql_get_all_records = "select code, tra_date, chip, close  from %s where  code=%s and length(chip)>4 and length(close)>=4 "%(table_name, code)  # 找出所有的chip和close不为空的记录
#    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table", port=3306, charset='utf8')
#
#    #conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,
#    #                       db='pv_table', charset='utf8')
#
#    cur = conn.cursor()
#    pdb.set_trace()
#    # 获取所有记录，一次性算完之后再写回到Mysql
#    cur.execute(sql_get_all_records)
#    records_tuple = cur.fetchall()

#    records=[]
#
##    pdb.set_trace()
#    for item in records_tuple[0:1]:
#        tmp_pv_table = eval(item[2])
#        close = item[3]    # code, tra_date, chip, close
#        pv_table = {}
#
#        for key, value in tmp_pv_table.items():
#            pv_table[float(key)] = value
#
#        sp_price_dict = extreme(pv_table, close)    # 需要获得前复权价格   002668 NoneType has no attribute 'item'
#
#        records.append((sp_price_dict["P"], sp_price_dict["S"], item[0], item[1]))
## 所有记录形成list
#
#    try:
#        if table_name == "pricetable_zb":
#            #db.cursor.executemany("insert into pricetable_zb (code, tra_date, close) values(%s,%s,%f)", records)
#            cur.executemany("update pricetable_zb set pre_p=%s, sup_p=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
#
#        if table_name == "pricetable_zxb":
#            #db.cursor.executemany("insert into pricetable_zxb (code, tra_date, close) values(%s,%s,%f)", records)
#            cur.executemany("update pricetable_zxb set pre_p=%s, sup_p=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
#        if table_name == "pricetable_cyb":
#            #db.cursor.executemany("insert into pricetable_cyb (code, tra_date, close) values(%s,%s,%f)", records)
#            cur.executemany("update pricetable_cyb set pre_p=%s, sup_p=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
#        conn.commit()
#        print(code, " over")
#    except Exception as e:
#        print("Exception: ", str(e))
#        conn.rollback()
#


    with getPTConnection() as db:
    #    sql_get_all_records = "select code, tra_date, chip, close  from %s where  code=%s and length(chip)>4 and length(close)>=4 "%(table_name, code)  # 找出所有的chip和close不为空的记录
        sql_get_all_records = "select code, tra_date, chip, close  from %s where code=%s"%(table_name, code)  # 找出所有的chip和close不为空的记录
    #conn = pymysql.connect(host='127.0.0.1', user='root', passwd='', db="pv_table", port=3306, charset='utf8')

    # 获取所有记录，一次性算完之后再写回到Mysql
        db.cursor.execute(sql_get_all_records)
        records_tuple = db.cursor.fetchall()

        records=[]

#        pdb.set_trace()
        for item in records_tuple:
            tmp_pv_table = eval(item[2])
            close = item[3]    # code, tra_date, chip, close
            pv_table = {}

            for key, value in tmp_pv_table.items():
                pv_table[float(key)] = value

            sp_price_dict = extreme(pv_table, close)    # 需要获得前复权价格   002668 NoneType has no attribute 'item'

            records.append((float(sp_price_dict["P"]),float(sp_price_dict["S"]), item[0], item[1]))

        try:
            if table_name == "pricetable_zb":
                #db.cursor.executemany("insert into pricetable_zb (code, tra_date, close) values(%s,%s,%f)", records)
                db.cursor.executemany("update pricetable_zb set pre_p=%s, sup_p=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))

            if table_name == "pricetable_zxb":
                #db.cursor.executemany("insert into pricetable_zxb (code, tra_date, close) values(%s,%s,%f)", records)
                db.cursor.executemany("update pricetable_zxb set pre_p=%s, sup_p=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
            if table_name == "pricetable_cyb":
                #db.cursor.executemany("insert into pricetable_cyb (code, tra_date, close) values(%s,%s,%f)", records)
                db.cursor.executemany("update pricetable_cyb set pre_p=%s, sup_p=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
            db.conn.commit()
            print(code, " over")
        except Exception as e:
            print("Exception: ", str(e))
            db.conn.rollback()


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



def cal_one_code_sp_price(code):    # 计算一只股票的支撑位和压力位
    #conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table", port=3306, charset='utf8')
    #cur = conn.cursor()
    code_table = {'6':"pricetable_zb", '0':"pricetable_zxb", '3':"pricetable_cyb"}
    table_name = code_table[code[0]]
    
    with getPTConnection() as db:
        if table_name == "pricetable_zb":
            db.cursor.execute("select tra_date, chip, close  from pricetable_zb where code=%s and tra_date<'20160101'"%(code))  # 找出所有的chip和close不为空的记录
        if table_name == "pricetable_zxb":
            db.cursor.execute("select tra_date, chip, close  from pricetable_zxb where code=%s and tra_date<'20160101'"%(code))     
        if table_name == "pricetable_cyb":
            db.cursor.execute("select tra_date, chip, close  from pricetable_cyb where code=%s and tra_date<'20160101'"%(code))

        records_tuple = db.cursor.fetchall()
        # 获取所有记录，一次性算完之后再写回到Mysql
        records = []
        for item in records_tuple:
            tmp_pv_table = eval(item[1])
            close = item[2]    # code, tra_date, chip, close
            pv_table = {}

            for key, value in tmp_pv_table.items():
                pv_table[float(key)] = value

            sp_price_dict = extreme(pv_table, close)    # 需要获得前复权价格   002668 NoneType has no attribute 'item'

            records.append((float(sp_price_dict["P"]),float(sp_price_dict["S"]), code, item[0]))

        try:
            if table_name == "pricetable_zb":
                #db.cursor.executemany("insert into pricetable_zb (code, tra_date, close) values(%s,%s,%f)", records)
                db.cursor.executemany("update pricetable_zb set pre_p=%s, sup_p=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))

            if table_name == "pricetable_zxb":
                #db.cursor.executemany("insert into pricetable_zxb (code, tra_date, close) values(%s,%s,%f)", records)
                db.cursor.executemany("update pricetable_zxb set pre_p=%s, sup_p=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
            if table_name == "pricetable_cyb":
                #db.cursor.executemany("insert into pricetable_cyb (code, tra_date, close) values(%s,%s,%f)", records)
                db.cursor.executemany("update pricetable_cyb set pre_p=%s, sup_p=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
            db.conn.commit()
            print(code, " over")
        except Exception as e:
            print("Exception: ", str(e))
            db.conn.rollback()
 
def cal_one_code_win_lose_ratio(code):    # 计算一只股票的支撑位和压力位的支撑强度和突破强度
    #conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table", port=3306, charset='utf8')
    #cur = conn.cursor()
    code_table = {'6':"pricetable_zb", '0':"pricetable_zxb", '3':"pricetable_cyb"}
    table_name = code_table[code[0]]
     
    

    with getPTConnection() as db:
        if table_name == "pricetable_zb":
            db.cursor.execute("select tra_date from pricetable_zb where code=%s and tra_date>'20160101'"%(code))  # 找出所有的tra_date,因为今天的收盘价比较的是昨天的支撑位和压力位
        if table_name == "pricetable_zxb":
            db.cursor.execute("select tra_date from pricetable_zxb where code=%s and tra_date>'20160101'"%(code))     
        if table_name == "pricetable_cyb":
            db.cursor.execute("select tra_date from pricetable_cyb where code=%s and tra_date>'20160101'"%(code))

        tra_dates = db.cursor.fetchall()    # 取得所有交易日
        # 获取所有记录，一次性算完之后再写回到Mysql
        pre_records = []  # 记录每一天的支撑压力有效情况
        sup_records = []


        for i in range(1, len(tra_dates)):
            try:
                db.cursor.execute("select pre_p, sup_p from %s where code='%s' and tra_date='%s'"%(table_name, code, tra_dates[i-1][0]))  #(11.47, 11.94) 
                sp_price = db.cursor.fetchone() 
                db.cursor.execute("select close from %s where code='%s' and tra_date='%s'"%(table_name, code, tra_dates[i][0]))    #(11.9403,) 
                close = db.cursor.fetchone() 
                if close[0] > sp_price[0]: # 收盘价大于压力位 压力无效
                    pre_records.append(0)
                else: # 压力有效
                    pre_records.append(1)

                if close[0] < sp_price[1]:  # 收盘价小于支撑位，支撑无效
                    sup_records.append(0)
                else: # 支撑无效
                    sup_records.append(1)
            except Exception as e:
                pre_records.append(0)
                sup_records.append(0)
                print("Exception: ", str(e))    
        
        # 将每一天的支撑压力情况统计成每一天的支撑压力信息
        pre_records = [1] + pre_records
        sup_records = [1] + sup_records
        
        pre_efforts = []        
        sup_efforts = []

        for i in range(1, len(pre_records)):
            pre_efforts.append(sum(pre_records[0:i])/i)   # 比如[1,0,1,1,1,0,1]  
            sup_efforts.append(sum(sup_records[0:i])/i)
        

        records = [(pre_efforts[i], sup_efforts[i], code, tra_dates[i]) for i in range(len(pre_efforts))]
        
               
        try:
            if table_name == "pricetable_zb":
                #db.cursor.executemany("insert into pricetable_zb (code, tra_date, close) values(%s,%s,%f)", records)
                db.cursor.executemany("update pricetable_zb set pre_effort=%s, sup_effort=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))

            if table_name == "pricetable_zxb":
                #db.cursor.executemany("insert into pricetable_zxb (code, tra_date, close) values(%s,%s,%f)", records)
                db.cursor.executemany("update pricetable_zxb set pre_effort=%s, sup_effort=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
            if table_name == "pricetable_cyb":
                #db.cursor.executemany("insert into pricetable_cyb (code, tra_date, close) values(%s,%s,%f)", records)
                db.cursor.executemany("update pricetable_cyb set pre_effort=%s, sup_effort=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
            db.conn.commit()
            print(code, " over")
        except Exception as e:
            print("Exception: ", str(e))
            db.conn.rollback()
 
#def cal_one_table_sp_price(table):
#    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table", port=3306, charset='utf8')
#    cur = conn.cursor()
#    sql_get_tables_from_table = "select distinct code from %s"%table
#    cur.execute(sql_get_tables_from_table)
#    row_list_codes = cur.fetchall()
##    pdb.set_trace()
#    #conn.close() 
#    pool = multiprocessing.Pool(processes=8)
#    for item in row_list_codes[0:8]:
#            #pool.apply_async(insert_close, (table, item[0], conn))
#        #sql_get_all_records = "select code, tra_date, chip, close  from %s where code=%s"%(table_name, code)  # 找出所有的chip和close不为空的记录
#        #code_table = {'6':"pricetable_zb", '0':"pricetable_zxb", '3':"pricetable_cyb"}
#        #table_name = code_table[item[0][0]]
# 
#        #sql_get_all_records = "select code, tra_date, chip, close  from %s where code=%s and tra_date<'20160101'"%(table_name, item[0])  # 找出所有的chip和close不为空的记录
#        #sql_get_all_records = "select code, tra_date, chip, close  from %s where  code=%s and length(chip)>4 and length(close)>=4 "%(table_name, code)  # 找出所有的chip和close不为空的记录
#        #conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table", port=3306, charset='utf8')
#    
#        #conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,
#        #                       db='pv_table', charset='utf8')
#    
#        #cur = conn.cursor()
#        #pdb.set_trace()
#        # 获取所有记录，一次性算完之后再写回到Mysql
#        #cur.execute(sql_get_all_records)
#        #records_tuple = cur.fetchall()
#        #pdb.set_trace()
# 
#        pool.apply_async(cal_one_stock_sp_price, (item[0], ))
#        #cal_one_stock_sp_price(item[0])  # 计算支撑压力位,支撑压力位的计算需要用到close
#    pool.close()
#    pool.join()
#    print(table, "over")

if __name__ == '__main__':
    # 第一步，读取pv_table库中table列表，既得股票名称列表
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table", port=3306, charset='utf8')
    #conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,
    #                       db='pv_table', charset='utf8')

    # select code, tra_date from pricetable_zb where code = '603999';
    # select distinct code from pricetable_zb;

    cur = conn.cursor()
#    sql_get_all_tables = "select code, tra_date, chip, close  from pricetable_zb where code='600000' and tra_date<'20160101'"
#    cur.execute(sql_get_all_tables)
#    a = cur.fetchall()
#        ## 类似格式 (("sh600000",),("sz000001", ))
    code_table = ["pricetable_zb", "pricetable_zxb", "pricetable_cyb"]
    codes = []

    for table in code_table:
        sql = "select distinct code from %s"%table
        cur.execute(sql)
        for item in cur.fetchall():
            codes.append(item[0])
    cur.close() 
    conn.close()    
#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$  计算支撑压力位
#    pool = multiprocessing.Pool(processes=8)
#    for code in codes:
#        pool.apply_async(cal_one_code_sp_price, (code,))     
#    pool.close()
#    pool.join() 

#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$   计算支撑压力强度
    pool = multiprocessing.Pool(processes=8)
    for code in codes:
        pool.apply_async(cal_one_code_win_lose_ratio, (code,))     
        #cal_one_code_win_lose_ratio(code)
    pool.close()
    pool.join() 


#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$   写入收盘价
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
#    for table in code_table:
#        cal_one_table_sp_price(table)        


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
