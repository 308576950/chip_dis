# -*- coding: utf-8 -*-

import pymysql
import numpy as np
import pandas as pd
import pdb
import multiprocessing


def choose_stocks(table_name, date):
    
    #conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_backup", port=3306, charset='utf8')
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table", port=3306, charset='utf8')
    cur = conn.cursor()
    #df = pd.DataFrame(columns=('code','tra_date','concentration'))
    tra_date = str(date).replace('-','')
    cur.execute("select code, close, chip from %s where tra_date=%s and chip_classify=3"%(table, tra_date))  
    rows = cur.fetchall()
   
    #pdb.set_trace()
    one_day_data = [] 

    for row in rows:
        code = row[0]
        close = row[1]
        tmp_chip = eval(row[2])
        weight_price = []
        cost_avg = 0
        for key, value in tmp_chip.items():
            weight_price.append(float(key) * float(value)) 
            cost_avg += float(key) * float(value)            

        concentration = np.array(weight_price).std()
        distance_p = (close - cost_avg)/cost_avg

        qianzhui_code = qianzhui[code[0]] + code    

        one_day_data.append((qianzhui_code, tra_date, concentration, distance_p))
        #df.loc[len(df) + 1] = [qianzhui_code, tra_date, concentration]
    print(table_name, ' ', date, ' over')
    return one_day_data
    #df.to_csv('/data/write_mysql_20180325/re_write/' + + '.csv')

if __name__ == '__main__':
    
    code_table = ["pricetable_zb", "pricetable_zxb", "pricetable_cyb"] 
    #conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_backup", port=3306, charset='utf8')
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table", port=3306, charset='utf8')
    cur = conn.cursor()

    qianzhui = {'6':'sh', '0':'sz', '3':'sz'}
    df = pd.DataFrame(columns=('code','tra_date','concentration', 'distance_p'))
    for table in  code_table:
        pool = multiprocessing.Pool(processes=8)
        sql_get_tables_from_table = "select distinct tra_date from %s where tra_date>'20160101'"%table
        cur.execute(sql_get_tables_from_table)
        row_list_dates = cur.fetchall()
        results = []
        for item in row_list_dates:
            #pool.apply_async(cal_one_code_avgcost_and_winpct, (table, item[0]))     
            #cal_one_code_avgcost_and_winpct(table, item[0])
            #pool.apply_async(cal_one_code_score, (table, item[0]))     
            #cal_one_code_score(table, item[0])
            if int(str(item[0]).replace('-','')) > 20160101:
                result = pool.apply_async(choose_stocks, (table, item[0]))
                results.append(result)
                #pool.apply_async(cal_chip_concentration, (table, item[0]))
                #result = cal_chip_concentration(table, item[0])
            #cal_chip_classify(table, item[0])
        pool.close()
        pool.join()
            
    
#        pdb.set_trace()
        for result in results:
            one_day_data = result.get()
            df = df.append(pd.DataFrame(one_day_data, columns=['code','tra_date','concentration','distance_p']))            
#            for item in one_day_data:                 # item   [qianzhui_code, tra_date, concentration]
#                df.loc[len(df) + 1] = list(item)        
#        pdb.set_trace()
    df.to_csv('/data/write_mysql_20180325/re_write/choose_stocks.csv') 


