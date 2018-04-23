import pymysql
import urllib.request
import json
import pandas as pd
import multiprocessing
import pdb
import math
#from DB_connetion_pool import getPTConnection, PTConnectionPool;

 
def cal_zone_win_lose(table_name, tra_date):    # 计算一只股票的支撑位和压力位的有效支撑天数和有效压力天数
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table", port=3306, charset='utf8')
    cur = conn.cursor()
#    code_table = {'6':"pricetable_zb", '0':"pricetable_zxb", '3':"pricetable_cyb"}
#    table_name = code_table[code[0]]
    records = []
    sql_get_tradates = "select code, close, pre_p, sup_p from %s where tra_date='%s'" % (table_name, tra_date)
    cur.execute(sql_get_tradates)   # 获得所有交易日
    row_date_chip_list = cur.fetchall()
    M = 5    
    N = 3
    #one_table_win_pct = [] 
    one_day_win_pct = []
    for i in range(M):
        one_day_win_pct.append([])
    
    tag_code = {}   # tag_code 最终形式  {'1': ['600000','000001']}
    zone_win_pct = {}        
    for j in range(1, M+1):
        tag_code[str(j)] = []
        zone_win_pct[str(j)] = []          # zone_win_pct 最终形式 {'1',54%, '2':56%} 

    code_close = {}
    for i in range(0, len(row_date_chip_list)):    # 从第三天开始计算，因为需要统计三天胜率
        code = row_date_chip_list[i][0]
        close = row_date_chip_list[i][1]
        pre_p = row_date_chip_list[i][2]            
        sup_p = row_date_chip_list[i][3]            
        code_close[code] = close        

        try: 
            #zone = math.ceil((close - sup_p)/(pre_p - sup_p)*100)    # 向上取整,便于分组 
            zone = math.ceil(math.ceil((close - sup_p)/(pre_p - sup_p)*100)/(100/M))
            # zone 为0到100之间的数字  接下来将zone分组 就是给code打标签  
            if zone in range(1, M+1): 
                tag_code[str(zone)].append(code)    # 将code打上zone 标签  {'1': ['600000']}
        except Exception as e:
            #print(str(e), code, tra_date, close, sup_p, pre_p)
            pass

    index = list_tra_date.index([str(tra_date), 1])  # 获取当天交易日的index，便于获取下3个交易日 
    for i in range(index+N, len(list_tra_date)):    # 从前三天开始找起p    3是一个参数
        if list_tra_date[i][-1] == 1:
            threedays_after  = list_tra_date[i][0].replace('-','')

    records = []
    for key,value in tag_code.items():  #  {'1': ['600000','000001'], '2':['300001', '600005']}
        for code in value:
            #records.append((table_name, code, threedays_after))
            records.append((code, threedays_after))
        #pdb.set_trace()
        if table == "pricetable_zb":
            cur.executemany("select code, close from pricetable_zb where code=%s and tra_date=%s", records)
        if table == "pricetable_zxb":
            cur.executemany("select code, close from pricetable_zxb where code=%s and tra_date=%s", records)
        if table == "pricetable_cyb":
            cur.executemany("select code, close from pricetable_cyb where code=%s and tra_date=%s", records)

        threedays_closes = cur.fetchall()
        for item in threedays_closes:
            code = item[0]
            threedays_close = item[1]    # 三天之后的价格
            if threedays_close:
                if threedays_close >= code_close[code]:
                # 胜
                    zone_win_pct[key].append(1)
                else:
                    zone_win_pct[key].append(0)
            
            #开始骚操作    

#    for key,value in tag_code.items():  #  {'1': ['600000','000001'], '2':['300001', '600005']}
#        for code in value:
#            # 统计三天之后的close
#            try:
#                sql_get_threedays_close = "select close from %s where code=%s and tra_date=%s "% (table_name, code, threedays_after)   # 三天之后
#                cur.execute(sql_get_threedays_close)
#                threedays_close = cur.fetchone()[0]
#            except Exception as e:
#                threedays_close = 0
#            
#            if threedays_close >= close:
#                # 胜
#                zone_win_pct[key].append(1) 
#            else:
#                zone_win_pct[key].append(0) 
#        
#        # 查看zone_win_pct的胜率
    zone_win_pct_oneday = {}
    

    for key, value in zone_win_pct.items():
        if len(value) > 0:
            zone_win_pct_oneday[key] = sum(value)/len(value)   # 最终得到 {'1':51%, '2':54%} 类似，1表示第一组，51% 表示该组的胜率
        #print(key, sum(value)/len(value))
    return zone_win_pct_oneday

#    pdb.set_trace()
#    print(tra_date, ' over')
#    for i in range(len(list(zone_win_pct_oneday.keys()))):
#        one_day_win_pct[i].append(list(zone_win_pct_oneday.keys())[i])           

#    for item in one_day_win_pct:   # 一个table的胜率  [[51%, 50%, 52%...],[51%, 50%, 52%...].[]]   第一组  第二组 等等  
#        print(sum(item) / len(item))



if __name__ == '__main__':
    # 从 20160101开始，第一函数每天将股票分组，输出日期和分组   第二个函数的输入参数为 一组股票和日期，该函数求出胜率
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table", port=3306, charset='utf8')
    #conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_backup", port=3306, charset='utf8')
    cur = conn.cursor()
    cur.execute("select distinct(tra_date) from pricetable_cyb order by tra_date desc limit 1")     # 取三天后的涨幅  最多只取到最新交易日的前三天
    last_day = str(cur.fetchall()[-1][0])


    url_tra_date = "http://fintech.jrj.com.cn/tp/astock/getholidays?start=1990-12-01&end=" + last_day
    res_tra_date = urllib.request.urlopen(url_tra_date)
    html_tra_date = res_tra_date.read()
    list_tra_date = json.loads(html_tra_date.decode('utf-8'))['data']

    
    code_table = ["pricetable_zb", "pricetable_zxb", "pricetable_cyb"]
    codes = []

    total_table_result = {}
    for table in code_table:       # 先只算zb
        pool = multiprocessing.Pool(processes=16)
        sql_get_tables_from_table = "select distinct tra_date from %s where tra_date>'20160101'"%table
        cur.execute(sql_get_tables_from_table)
        row_list_codes = cur.fetchall()
        onetable_result = {}
        oneday_results = []
        for i in range(len(row_list_codes)-3):
            result = pool.apply_async(cal_zone_win_lose, (table, row_list_codes[i][0]))     
            #oneday_result = cal_zone_win_lose(table, row_list_codes[i][0])
            oneday_results.append(result)
        pool.close()
        pool.join()
        print('table cal over')
#        pdb.set_trace()
        for j in range(len(oneday_results)):
        #for oneday_result_get in oneday_results:
            oneday_result = oneday_results[j].get()
#            oneday_result = oneday_result_get.get()
#            oneday_result = oneday_result_async.get()
            for key, value in oneday_result.items():
                if key in onetable_result.keys():
                    onetable_result[key].append(value)
                else:
                    onetable_result[key] = []
        for key, value in onetable_result.items():
            print(key, sum(value)/len(value))
            if key in total_table_result.keys():
                total_table_result[key] = total_table_result[key] + sum(value)/len(value)
            else:
                total_table_result[key] = sum(value)/len(value)         
    print(total_table_result)     




