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


def insert_close(table_name, code):   # 增加close_price字段
    # 思路  一次读取完所有的价格 然后executemany
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_backup", port=3306, charset='utf8')
    cur = conn.cursor()
    records = []
    
#    if code == '002593':
#        pdb.set_trace()
#        pass    

#    pdb.set_trace() 
    sql_get_tradates = "select tra_date from %s where code = '%s' and tra_date<'20160101'" % (table_name, code)
    cur.execute(sql_get_tradates)   # 获得所有交易日
    row_date_list = cur.fetchall()

    file_name = {'6':'.SH.CSV', '0':".SZ.CSV", '3':".SZ.CSV"}    

    df = pd.read_csv("/data/write_mysql_20180325/re_write/front_exclude_close/" + code + file_name[code[0]], index_col=2, encoding="gbk")
    

    for item in row_date_list:
        try:
            close_price = df.loc[int(str(item[0]).replace('-','')),'收盘价(元)'].round(2) # 保留两位有效数字
        except Exception as e:
            print("Exception: ", str(e))
            close_price = 0.0

        if close_price == 0.0:
            pdb.set_trace()
            pass
        records.append((float(close_price), code, str(item[0]).replace('-','')))   # 读取记录，存入tuple中
#    pdb.set_trace() 
 
#    with getPTConnection() as db:
    try:
        if table_name == "pricetable_zb":
            #db.cursor.executemany("insert into pricetable_zb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_zb set close=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))

        if table_name == "pricetable_zxb":
            #db.cursor.executemany("insert into pricetable_zxb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_zxb set close=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
        if table_name == "pricetable_cyb":
            #db.cursor.executemany("insert into pricetable_cyb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_cyb set close=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
        conn.commit()
        print(code, " over")
    except Exception as e:
        print("Exception: ", str(e))
        conn.rollback()


def format_chip(table_name, code):   #将chip进行格式化, value保留8位有效数字 
    # 思路  一次读取完所有的价格 然后executemany
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_backup", port=3306, charset='utf8')
    cur = conn.cursor()
    records = []
    sql_get_tradates = "select tra_date, chip from %s where code = '%s'" % (table_name, code)
    cur.execute(sql_get_tradates)   # 获得所有交易日
    row_date_chip_list = cur.fetchall()
    

    file_name = {'6':'.SH.CSV', '0':".SZ.CSV", '3':".SZ.CSV"}    

    for item in row_date_chip_list:
        try:
            original_chip = eval(item[1])
        except Exception as e:
            original_chip = {}

        chip = {}
        if original_chip:
            for key, value in original_chip.items():
                chip[str(round(float(key), 2))] = str("%.8f"%float(value))                      # 价格保留2位小数，占比保留8位有效数字
            

        records.append((str(chip), code, str(item[0]).replace('-','')))   # 读取记录，存入tuple中
#    print(records[-1])
    try:
        if table_name == "pricetable_zb":
            #db.cursor.executemany("insert into pricetable_zb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_zb set chip=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))

        if table_name == "pricetable_zxb":
            #db.cursor.executemany("insert into pricetable_zxb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_zxb set chip=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
        if table_name == "pricetable_cyb":
            #db.cursor.executemany("insert into pricetable_cyb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_cyb set chip=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
        conn.commit()
        print(code, " over")
    except Exception as e:
        print("Exception: ", str(e))
        conn.rollback()




def cal_one_day_sp_price(table, date):  # 计算一天的支撑压力位
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_backup", port=3306, charset='utf8')
    cur = conn.cursor()
    
#    tables = ["pricetable_zb", "pricetable_zxb", "pricetable_cyb"]

#    for item in tables:
#    pdb.set_trace()
    records = []
    cur.execute("select code, chip, close from %s where tra_date=%s"%(table, date))
    row = cur.fetchall()   # 该天该table内的所有股票的记录
    for iitem in row:
        code = iitem[0]
        chip = eval(iitem[1])
        close = iitem[2]
        
        pv_table = {}
        for key, value in chip.items():
            pv_table[float(key)] = value
        sp_price_dict = extreme(pv_table, close)
        records.append((float(sp_price_dict["P"]),float(sp_price_dict["S"]), code, date))

    if records:
        try:
            if table == "pricetable_zb":
                #db.cursor.executemany("insert into pricetable_zb (code, tra_date, close) values(%s,%s,%f)", records)
                cur.executemany("update pricetable_zb set pre_p=%s, sup_p=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))

            if table == "pricetable_zxb":
                #db.cursor.executemany("insert into pricetable_zxb (code, tra_date, close) values(%s,%s,%f)", records)
                cur.executemany("update pricetable_zxb set pre_p=%s, sup_p=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
            if table == "pricetable_cyb":
                #db.cursor.executemany("insert into pricetable_cyb (code, tra_date, close) values(%s,%s,%f)", records)
                cur.executemany("update pricetable_cyb set pre_p=%s, sup_p=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
            conn.commit()
            print(table, date, " over")
        except Exception as e:
            print("Exception: ", str(e))
            conn.rollback()
    else:
        print(code, " 在20160101之前无数据")






def cal_one_code_sp_price(code):    # 计算一只股票的支撑位和压力位
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_backup", port=3306, charset='utf8')
    cur = conn.cursor()
    code_table = {'6':"pricetable_zb", '0':"pricetable_zxb", '3':"pricetable_cyb"}
    table_name = code_table[code[0]]
   
    # 一次性读取太多，磁盘IO太慢，需要分批次读取。
 
#    with getPTConnection() as db:
    if table_name == "pricetable_zb":
        cur.execute("select tra_date, chip, close  from pricetable_zb where code=%s and tra_date<'20160101'"%(code))  # 找出所有的chip和close不为空的记录
    if table_name == "pricetable_zxb":
        cur.execute("select tra_date, chip, close  from pricetable_zxb where code=%s and tra_date<'20160101'"%(code))     
    if table_name == "pricetable_cyb":
        cur.execute("select tra_date, chip, close  from pricetable_cyb where code=%s and tra_date<'20160101'"%(code))

    #pdb.set_trace()
    #print("读取完毕")
    records_tuple = cur.fetchall()
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
    
    if records:
        try:
            if table_name == "pricetable_zb":
                #db.cursor.executemany("insert into pricetable_zb (code, tra_date, close) values(%s,%s,%f)", records)
                cur.executemany("update pricetable_zb set pre_p=%s, sup_p=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))

            if table_name == "pricetable_zxb":
                #db.cursor.executemany("insert into pricetable_zxb (code, tra_date, close) values(%s,%s,%f)", records)
                cur.executemany("update pricetable_zxb set pre_p=%s, sup_p=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
            if table_name == "pricetable_cyb":
                #db.cursor.executemany("insert into pricetable_cyb (code, tra_date, close) values(%s,%s,%f)", records)
                cur.executemany("update pricetable_cyb set pre_p=%s, sup_p=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
            conn.commit()
            print(code, " over")
        except Exception as e:
            print("Exception: ", str(e))
            conn.rollback()
    else:
        print(code, " 在20160101之前无数据")

 
def cal_one_code_win_lose_day(table_name, code):    # 计算一只股票的支撑位和压力位的有效支撑天数和有效压力天数
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_backup", port=3306, charset='utf8')
    cur = conn.cursor()
#    code_table = {'6':"pricetable_zb", '0':"pricetable_zxb", '3':"pricetable_cyb"}
#    table_name = code_table[code[0]]
    records = []
    sql_get_tradates = "select tra_date, close, pre_p, sup_p from %s where code = '%s'" % (table_name, code)
    cur.execute(sql_get_tradates)   # 获得所有交易日
    row_date_chip_list = cur.fetchall()
    
# 先要增加字段

    file_name = {'6':'.SH.CSV', '0':".SZ.CSV", '3':".SZ.CSV"}    
    
    pre_records = []  # 记录每一天的支撑压力有效情况
    sup_records = []

    
    for i in range(1, len(row_date_chip_list)):    # item[0,1,2,3]
        date = row_date_chip_list[i][0]
        close = row_date_chip_list[i][1]
        pre_p = row_date_chip_list[i][2]
        sup_p = row_date_chip_list[i][3]            

        if close <= pre_p:
            pre_records.append(1)
        else:
            pre_records.append(0)
    
        if close >= sup_p:
            sup_records.append(1)
        else:
            sup_records.append(0)

    pre_records = [1] + pre_records
    sup_records = [1] + sup_records

    pre_yes = []
    sup_yes = []

    for i in range(len(pre_records)):
        pre_yes.append(sum(pre_records[0:i]))   # 比如[1,0,1,1,1,0,1]  
        sup_yes.append(sum(sup_records[0:i]))


    #pdb.set_trace()
    records = [(pre_yes[i], sup_yes[i], len(row_date_chip_list), code, row_date_chip_list[i][0]) for i in range(len(row_date_chip_list))]


#        records.append((str(chip), code, str(item[0]).replace('-','')))   # 读取记录，存入tuple中
#    print(records[-1])
    try:
        if table_name == "pricetable_zb":
            #db.cursor.executemany("insert into pricetable_zb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_zb set presbit_number=%s, supbit_number=%s,total_number=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))

        if table_name == "pricetable_zxb":
            #db.cursor.executemany("insert into pricetable_zxb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_zxb set presbit_number=%s, supbit_number=%s,total_number=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
        if table_name == "pricetable_cyb":
            #db.cursor.executemany("insert into pricetable_cyb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_cyb set presbit_number=%s, supbit_number=%s,total_number=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
        conn.commit()
        print(code, " over")
    except Exception as e:
        print("Exception: ", str(e))
        conn.rollback()

    #pdb.set_trace()


def cal_one_code_avgcost_and_winpct(table_name, code):    # 计算一只股票的平均成本、获利比例、每一价格处获利比例
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_backup", port=3306, charset='utf8')
    cur = conn.cursor()
#    code_table = {'6':"pricetable_zb", '0':"pricetable_zxb", '3':"pricetable_cyb"}
#    table_name = code_table[code[0]]
    records = []
    sql_get_tradates = "select tra_date, chip, close from %s where code = '%s' and tra_date>'20180101'" % (table_name, code)
    cur.execute(sql_get_tradates)   # 获得所有交易日
    row_date_chip_list = cur.fetchall()
    
# 先要增加字段

    file_name = {'6':'.SH.CSV', '0':".SZ.CSV", '3':".SZ.CSV"}    
    
    pre_records = []  # 记录每一天的支撑压力有效情况
    sup_records = []

 
    for i in range(1, len(row_date_chip_list)):    # item[0,1,2,3]
        date = row_date_chip_list[i][0]        #日期
        chip = eval(row_date_chip_list[i][1])  #筹码
        close = row_date_chip_list[i][2]
        avg_cost = 0
        tmp_chip = {}
        for key, value in chip.items():
            avg_cost = avg_cost + float(key) * float(value)
            tmp_chip[float(key)] = float(value)
        ttmp_chip = [(k,tmp_chip[k]) for k in sorted(tmp_chip.keys())]
        sorted_keys = [item[0] for item in ttmp_chip]                      #list(ttmp_chip.keys())
        sorted_values = [item[1] for item in ttmp_chip]                 #list(ttmp_chip.values())

        price_win_pct = []           # 某一价格获利比例
        win_pct = 0                  # 获利比例

        for key,value in chip.items():
            index = sorted_keys.index(float(key))
            sum_zhanbi = sum(sorted_values[:index])
            price_win_pct.append(sum_zhanbi)
            if round(float(key),2) == close:
                win_pct = sum(sorted_values[:index])
         
        #  propct_cerprc mediumtext, add profit_pct float, add cost_avg
        #records = [(str(price_win_pct), win_pct, avg_cost, code, date)]
        records.append((str(price_win_pct), win_pct, avg_cost, code, date))

#        records.append((str(chip), code, str(item[0]).replace('-','')))   # 读取记录，存入tuple中
#    print(records[-1])
    try:
        if table_name == "pricetable_zb":
            #db.cursor.executemany("insert into pricetable_zb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_zb set propct_cerprc=%s, profit_pct=%s, cost_avg=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))

        if table_name == "pricetable_zxb":
            #db.cursor.executemany("insert into pricetable_zxb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_zxb set propct_cerprc=%s, profit_pct=%s, cost_avg=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
        if table_name == "pricetable_cyb":
            #db.cursor.executemany("insert into pricetable_cyb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_cyb set propct_cerprc=%s, profit_pct=%s, cost_avg=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
        conn.commit()
        print(code, " over")
    except Exception as e:
        print("Exception: ", str(e))
        conn.rollback()


def cal_one_code_score(table_name, code):    # 计算一只股票的支撑位和压力位的有效支撑天数和有效压力天数
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_backup", port=3306, charset='utf8')
    cur = conn.cursor()
#    code_table = {'6':"pricetable_zb", '0':"pricetable_zxb", '3':"pricetable_cyb"}
#    table_name = code_table[code[0]]
    records = []
    sql_get_tradates = "select close, sup_p, pre_p, tra_date from %s where code = '%s' and tra_date>'20180101'" % (table_name, code)
    cur.execute(sql_get_tradates)   # 获得所有交易日
    row_date_chip_list = cur.fetchall()
    
# 先要增加字段
    
    for item in row_date_chip_list:    # item[0,1,2,3]
        close = item[0]
        sup_p = item[1]
        pre_p = item[2]
        tra_date = item[3]

        avg = (sup_p + pre_p)/2
        try:
            score = 5 + (close - avg)/(sup_p + pre_p)
        except Exception as e:
            print(code, tra_date, str(e))
            score = 5
        
 
        #  propct_cerprc mediumtext, add profit_pct float, add cost_avg
        records.append((score, code, str(tra_date).replace('-','')))


#    pdb.set_trace()
#        records.append((str(chip), code, str(item[0]).replace('-','')))   # 读取记录，存入tuple中
#    print(records[-1])
    try:
        if table_name == "pricetable_zb":
            #db.cursor.executemany("insert into pricetable_zb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_zb set score=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))

        if table_name == "pricetable_zxb":
            #db.cursor.executemany("insert into pricetable_zxb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_zxb set score=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
        if table_name == "pricetable_cyb":
            #db.cursor.executemany("insert into pricetable_cyb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_cyb set score=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
        conn.commit()
        print(code, " over")
    except Exception as e:
        print("Exception: ", str(e))
        conn.rollback()


def cal_chip_classify(table_name, code):
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_backup", port=3306, charset='utf8')
    cur = conn.cursor()
#    code_table = {'6':"pricetable_zb", '0':"pricetable_zxb", '3':"pricetable_cyb"}
#    table_name = code_table[code[0]]
    records = []
    sql_get_tradates = "select chip, close, tra_date  from %s where code = '%s' and tra_date>'201i60101'" % (table_name, code)

    cur.execute(sql_get_tradates)   # 获得所有交易日
    row_date_chip_list = cur.fetchall()
    
# 先要增加字段
    
    for item in row_date_chip_list:    # item[0,1,2,3]
        chip = eval(item[0])
        close = item[1]
        tra_date = item[2]
        avg_p = 0
        sum_below_close = 0
        for key,value in chip.items():
            avg_p += float(key) * float(value)
            if float(key) <= close:
                sum_below_close += float(value)    # sum_below_close是在close之下的筹码占比和

        chip_classify = 2

        if avg_p < close * 0.9 and sum_below_close >= 0.6:   # 向下集中
            chip_classify = 3
        if avg_p > close * 1.1 and 1-sum_below_close >= 0.6: # 向上集中
            chip_classify = 1;

       


        #  propct_cerprc mediumtext, add profit_pct float, add cost_avg
        records.append((chip_classify, code, str(tra_date).replace('-','')))


#    pdb.set_trace()
#        records.append((str(chip), code, str(item[0]).replace('-','')))   # 读取记录，存入tuple中
#    print(records[-1])
    try:
        if table_name == "pricetable_zb":
            #db.cursor.executemany("insert into pricetable_zb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_zb set chip_classify=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))

        if table_name == "pricetable_zxb":
            #db.cursor.executemany("insert into pricetable_zxb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_zxb set chip_classify=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
        if table_name == "pricetable_cyb":
            #db.cursor.executemany("insert into pricetable_cyb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_cyb set chip_classify=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
        conn.commit()
        print(code, " over")
    except Exception as e:
        print("Exception: ", str(e))
        conn.rollback()





if __name__ == '__main__':
    # 第一步，读取pv_table库中table列表，既得股票名称列表
    #conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table", port=3306, charset='utf8')
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_backup", port=3306, charset='utf8')

    # select code, tra_date from pricetable_zb where code = '603999';
    # select distinct code from pricetable_zb;

    cur = conn.cursor()
#    sql_get_all_tables = "select code, tra_date, chip, close  from pricetable_zb where code='600000' and tra_date<'20160101'"
#    cur.execute(sql_get_all_tables)
#    a = cur.fetchall()
#        ## 类似格式 (("sh600000",),("sz000001", ))
    code_table = ["pricetable_zb", "pricetable_zxb", "pricetable_cyb"]
    codes = []

#    for table in code_table:
#        sql = "select distinct code from %s"%table
#        cur.execute(sql)
#        for item in cur.fetchall():
#            codes.append(item[0])
      
#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$  计算支撑压力位
#    print("all codes read ovet")
    

#    cal_one_day_sp_price('pricetable_zb', '20151231')

#    for table in code_table:
#        pool = multiprocessing.Pool(processes=8)
#        cur.execute("select distinct tra_date from %s where tra_date<20160101 order by tra_date desc"%table)
#        row = cur.fetchall()
#        for item in row:
#            #cal_one_day_sp_price(code_table[0], str(item[0]).replace('-',''))
#            pool.apply_async(cal_one_day_sp_price, (table, str(item[0]).replace('-','')))     
#
#   #for code in codes[0:1]:
#        #pool.apply_async(cal_one_code_sp_price, (code,))     
#        #cal_one_code_sp_price(code)
#        pool.close()
#        pool.join() 

#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$   计算支撑压力强度
#    for table in code_table:
#        pool = multiprocessing.Pool(processes=16)
#        sql_get_tables_from_table = "select distinct code from %s"%table
#        cur.execute(sql_get_tables_from_table)
#        row_list_codes = cur.fetchall()
#        for item in row_list_codes:
#            pool.apply_async(cal_one_code_win_lose_day, (table, item[0]))     
#           #cal_one_code_win_lose_day(table, item[0])
#        pool.close()
#        pool.join() 

#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$   计算筹码分类
    for table in code_table:
        pool = multiprocessing.Pool(processes=16)
        sql_get_tables_from_table = "select distinct code from %s"%table
        cur.execute(sql_get_tables_from_table)
        row_list_codes = cur.fetchall()
        for item in row_list_codes:
            #pool.apply_async(cal_one_code_avgcost_and_winpct, (table, item[0]))     
#            cal_one_code_avgcost_and_winpct(table, item[0])
#            cal_one_code_score(table, item[0])
            cal_chip_classify(table, item[0])
        pool.close()
        pool.join() 
#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$








#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$   写入收盘价
#    for table in code_table[1:]:
#        pool = multiprocessing.Pool(processes=8)
#        sql_get_tables_from_table = "select distinct code from %s"%table
#        cur.execute(sql_get_tables_from_table)
#        row_list_codes = cur.fetchall()
#        for item in row_list_codes:
#            pool.apply_async(insert_close, (table, item[0]))
#            #insert_close(table, item[0])               # 增加close字段
#
#        pool.close()
#        pool.join()

#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$   格式化chip 
#    for table in code_table[1:]:
#        pool = multiprocessing.Pool(processes=8)
#        sql_get_tables_from_table = "select distinct code from %s"%table
#        cur.execute(sql_get_tables_from_table)
#        row_list_codes = cur.fetchall()
#        for item in row_list_codes:
#            pool.apply_async(format_chip, (table, item[0]))
#            #format_chip(table, item[0])               # 增加close字段
#
#        pool.close()
#        pool.join()


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
