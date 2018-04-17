import numpy as np
from heapq import nlargest
import pymysql
import math
import pandas as pd
import multiprocessing
import pdb
from DB_connetion_pool import getPTConnection, PTConnectionPool;


 
def cal_zone_win_lose(table_name, tra_date):    # 计算一只股票的支撑位和压力位的有效支撑天数和有效压力天数
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table", port=3306, charset='utf8')
    cur = conn.cursor()
#    code_table = {'6':"pricetable_zb", '0':"pricetable_zxb", '3':"pricetable_cyb"}
#    table_name = code_table[code[0]]
    records = []
    sql_get_tradates = "select code, tra_date, close, pre_p, sup_p from %s where tra_date= %s" % (table_name, tra_date)
    cur.execute(sql_get_tradates)   # 获得所有交易日
    row_date_chip_list = cur.fetchall()
    M = 100    

    one_table_win_pct = [] 
    for i in range(M):
        one_day_win_pct[i] = []


    for i in range(0, len(row_date_chip_list)-3):    # 从第三天开始计算，因为需要统计三天胜率
        tag_code = {}   # tag_code 最终形式  {'1': ['600000','000001']}
        zone_win_pct = {}        
        for i in range(1,101):
            tag_code[str(i)] = []
            zone_win_pct[str(i)] = []             


        code = row_date_chip_list[i][0]
        tra_date = row_date_chip_list[i][1]
        close = row_date_chip_list[i][2]
        pre_p = row_date_chip_list[i][3]            
        sup_p = row_date_chip_list[i][4]            
    
        zone = math.ceil((close - sup_p)/(pre_p - sup_p)*100)    # 向上取整,便于分组 
        # zone 为0到100之间的数字  接下来将zone分组 就是给code打标签   
        tag_code[str(zone)].append(code)    # 将code打上zone 标签  {'1': ['600000']}

        
        index = list_tra_date.index([tra_date, 1])  # 获取当天交易日的index，便于获取下3个交易日 
        for i in range(index+3, len(list_tra_date)):    # 从前三天开始找起p    3是一个参数
            if list_tra_date[i][-1] == 2:
                threedays_ago  = list_tra_date[i][0]

#y        records = []
#        for key,value in tag_code.items():  #  {'1': ['600000','000001'], '2':['300001', '600005']}
#            for code in value:
#                records.append((table_name, code, tra_date))
#
#        cur.executemany("select close from %s where code=%s and tra_date=%s", records)
#        threedays_close = cut.fetchall()
#        for item in threedays_close:
            #开始骚操作    

        for key,value in tag_code.items():  #  {'1': ['600000','000001'], '2':['300001', '600005']}
            for code in value:
            # 统计三天之后的close
                try:
                    sql_get_threedays_close = "select close from %s where code=%s and tra_date=%s "% (table_name, code, tra_date)   # 三天之后
                    cur.execute(sql_get_threedays_close)
                    threedays_close = cur.fetchone()[0]
                except Exception as e:
                    threedays_close = 0
                
                if threedays_close >= close:
                    # 胜
                    zone_win_pct[key].append(1) 
                else：
                    zone_win_pct[key].append(0) 
        
        # 查看zone_win_pct的胜率
        zone_win_pct_oneday = {}
    
        for key,value in zone_win_pct.items():
            zone_win_pct_oneday[key] = sum(value)/len(value)   # 最终得到 {'1':51%, '2':54%} 类似，1表示第一组，51% 表示该组的胜率
 
        for i in range(len(list(zone_win_pct_oneday.keys()))): 
            one_day_win_pct[i].append(list(zone_win_pct_oneday.keys())[i])           

    for item in one_day_win_pct:   # 一个table的胜率  [[51%, 50%, 52%...],[51%, 50%, 52%...].[]]   第一组  第二组 等等  
        print(sum(item) / len(item))


def cal_one_code_avgcost_and_winpct(table_name, code):    # 计算一只股票的支撑位和压力位的有效支撑天数和有效压力天数
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
        records = [(str(price_win_pct), win_pct, avg_cost, code, date)]


#    pdb.set_trace()
#        records.append((str(chip), code, str(item[0]).replace('-','')))   # 读取记录，存入tuple中
#    print(records[-1])
    try:
        if table_name == "pricetable_zb":
            #db.cursor.executemany("insert into pricetable_zb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_zb set propct_cerprc=%s, profit_pct=%s, cost_avg=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))

        if table_name == "pricetable_zxb":
            #db.cursor.executemany("insert into pricetable_zxb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_zb set propct_cerprc=%s, profit_pct=%s, cost_avg=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
        if table_name == "pricetable_cyb":
            #db.cursor.executemany("insert into pricetable_cyb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_zxb set presbit_number=%s, supbit_number=%s,total_number=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
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
        score = 5 + (close - avg)/(sup_p + pre_p)
        
        
 
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
            cur.executemany("update pricetable_zb set score=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
        if table_name == "pricetable_cyb":
            #db.cursor.executemany("insert into pricetable_cyb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_zxb set score=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
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
    sql_get_tradates = "select chip, close, tra_date  from %s where code = '%s' and tra_date>'20180401'" % (table_name, code)
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
            cur.executemany("update pricetable_zb set chip_classify=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
        if table_name == "pricetable_cyb":
            #db.cursor.executemany("insert into pricetable_cyb (code, tra_date, close) values(%s,%s,%f)", records)
            cur.executemany("update pricetable_zxb set chip_classify=%s where code=%s and tra_date=%s", records) #%(records[0][2], records[0][0], str(records[0][1]).replace('-','')))
        conn.commit()
        print(code, " over")
    except Exception as e:
        print("Exception: ", str(e))
        conn.rollback()





if __name__ == '__main__':
    # 从 20160101开始，第一函数每天将股票分组，输出日期和分组   第二个函数的输入参数为 一组股票和日期，该函数求出胜率

    url_tra_date = "http://fintech.jrj.com.cn/tp/astock/getholidays?start=1990-12-01&end=2018-03-14"
    res_tra_date = urllib.request.urlopen(url_tra_date)
    html_tra_date = res_tra_date.read()
    list_tra_date = json.loads(html_tra_date.decode('utf-8'))['data']




    #conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table", port=3306, charset='utf8')
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_backup", port=3306, charset='utf8')


    cur = conn.cursor()
    code_table = ["pricetable_zb", "pricetable_zxb", "pricetable_cyb"]
    codes = []

    for table in code_table:
        pool = multiprocessing.Pool(processes=8)
        sql_get_tables_from_table = "select distinct tra_date from %s where tra_date>'20160101'"%table
        cur.execute(sql_get_tables_from_table)
        row_list_codes = cur.fetchall()
        for item in row_list_codes:
            pool.apply_async(cal_zone_win_lose, (table, item[0]))     
           #cal_zone_win_lose(table, item[0])
        pool.close()
        pool.join() 





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
#        pool = multiprocessing.Pool(processes=8)
#        sql_get_tables_from_table = "select distinct code from %s"%table
#        cur.execute(sql_get_tables_from_table)
#        row_list_codes = cur.fetchall()
#        for item in row_list_codes:
#            pool.apply_async(cal_one_code_win_lose_day, (table, item[0]))     
#           #cal_one_code_win_lose_day(table, item[0])
#        pool.close()
#        pool.join() 

#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$   计算筹码分类
#    for table in code_table:
#        pool = multiprocessing.Pool(processes=8)
#        sql_get_tables_from_table = "select distinct code from %s"%table
#        cur.execute(sql_get_tables_from_table)
#        row_list_codes = cur.fetchall()
#        for item in row_list_codes:
#            #pool.apply_async(cal_one_code_avgcost_and_winpct, (table, item[0]))     
#            #cal_one_code_avgcost_and_winpct(table, item[0])
#            #cal_one_code_score(table, item[0])
#            cal_chip_classify(table, item[0])
#        #pool.close()
#        #pool.join() 
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
