# 计算筹码分布的同时也计算写入close，计算支撑压力位

import json
import urllib.request
import pandas as pd
import datetime
import pymysql
import multiprocessing
import time
import os
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor, as_completed
import pdb
from warnings import filterwarnings
from functools import reduce
#from DB_connetion_pool_pv_table_back_up import getPTConnection, PTConnectionPool;
import numpy as np
from heapq import nlargest
import sys

filterwarnings('ignore', category=pymysql.Warning)

def extreme(tmp_my_dict, close):   # 思路是在收盘价的一个涨跌幅之内最大的筹码密集区
    my_dict = {}
    for key, value in tmp_my_dict.items():
        my_dict[float(key)] = float(value)
    
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



def sell_prob(pv_table_keys, chip_keys, ratio, date):
    prob = []
    #pdb.set_trace()
    for i in pv_table_keys:
        tmp = [(j - i) / j for j in chip_keys]  # 每个pv_table_keys中的价格相对于chip_keys中的价格的收益
        prob.append(sum(tmp))

    index_positive = []
    earning_positive = []
    index_negative = []
    earning_negative_1 = []

    for i in range(0, len(prob)):
        if prob[i] >= 0:
            earning_positive.append(prob[i])
            index_positive.append(i)  # 记录该元素在原list中的位置
        else:
            earning_negative_1.append(prob[i])
            index_negative.append(i)  # 记录该元素在原list中的位置

    if earning_negative_1:  # earning_negative_1 为空的话就不用乘以e的系数
        prob_positive = [ratio * i / sum(earning_positive) for i in earning_positive]  # 具备正收益的价格的卖出概率
    else:
        prob_positive = [i / sum(earning_positive) for i in earning_positive]

    earning_negative_2 = [j - sum(earning_negative_1) for j in
                          earning_negative_1]  # 当只有一个负值时，此时earning_negative_2会变成[0.0]

    if earning_positive and len(earning_negative_2) != 1:  # earning_positive 为空说明全是负的收益，则不用乘以系数
        prob_negative = [(1 - ratio) * j / sum(earning_negative_2) for j in earning_negative_2]
    elif len(earning_negative_2) == 1:  # len(earning_negative_2) == 1 说明只有一个负值，此时earning_negative_2 = [0.0]，无法作为除数
        prob_negative = [1 - ratio]
    else:
        prob_negative = [j / sum(earning_negative_2) for j in earning_negative_2]

    probb = []
    for i in range(0, len(prob)):
        if i in index_positive:
            probb.append(prob_positive[index_positive.index(i)])
        else:
            try:
                probb.append(prob_negative[index_negative.index(i)])
            except Exception as e:
                print("Error", e)
    return probb


def cal_pvtable(tmp_pv_table, ddf, date, code):   # 利用昨天筹码图，当天分价表和date来计算date
    if code[0] == '6':
        tmp_code = code + '.SH'
    else:
        tmp_code = code + ".SZ"  # tmp_code = lambda x: x + '.SH' if x[0] == '6' else x + '.SZ'
    #dddf = pd.read_csv("/root/project_price/vol_turnover_test_ex_factor/" + tmp_code + ".CSV", encoding='gbk',index_col=3)  # 读取包含换手率、成交量和复权因子的CSV文件
    dddf = pd.read_csv("/root/project_price/vol_turnover_test_ex_factor_20180404/" + tmp_code + ".CSV", encoding='gbk',index_col=0)  # 读取包含换手率、成交量和复权因子的CSV文件
    ## 需要计算出从date日期到现在时间的累积复权因子
    #try:
    #    cum_factor = reduce(lambda x, y: x * y, dddf.loc[int(date)+1:, "复权因子"])    # 从该天之后的累计复权因子，用来将Tick数据中的真实价格转化成相对于此时此刻的前复权价格     
    #except Exception as e:
    #    cum_factor = 1
    #    print(str(e), code, " no cum_factor")

    #pdb.set_trace()
    try:
        ex_factor = dddf.loc[int(date), "复权因子"]
        turnover_ratio = float(dddf.loc[int(date), "换手率(%)"]) / 100  # 换手率
        turnover_volume = float(dddf.loc[int(date), "成交量(股)"])  # 成交量

        close_price = round(dddf.loc[int(date), '收盘价(元)'], 2)           # 原来的front_ex_close文件和现在的vol_turnover_test_ex_factor_20180404文件合并，close_price亦写在vol_turnover_test_ex_factor_20180404中, 有些价格是有四位有效数字，需要保留2位 

        # market_cap = turnover_volume / turnover_ratio

        pv_table = {}   # 传进来的tmp_pv_table的key是str类型
        for key, value in tmp_pv_table.items():
            pv_table[float(key)] = float(value)

        

        price = [round(i/ex_factor, 2) for i in list(ddf["Price"] / 10000)]
        #price = list(ddf["Price"] / 10000)     # 除以cum_factor, 比如从20160104之后只分红过两次，一次是16年6月17，一次是17年7月16，则cum_factor 等于这两次的除权因子的乘积。参加300299.
        volume = [i * turnover_ratio / turnover_volume for i in list(ddf["Volume"])]
        chip = dict(zip(price, volume))  # 直接形成chip表
        #if ddf["TotalTx"].sum() == 0:
        #    ratio = 
        ratio = ddf["Will"].sum() / ddf["TotalTx"].sum()
        # if pv_table:  # 根据分价表更新当天的筹码分布图
        # 分价表的数据是无关乎除权复权的，因此需要先检查筹码分布表

#        if ex_factor != 1:
#            tmp = {v: k for k, v in pv_table.items()}  # pv_table 和value 反过来
#            ttmp = {k: v / ex_factor for k, v in tmp.items()}  # 调整除权
#            pv_table = {round(v, 2): k for k, v in ttmp.items()}  # 再反过来

    #    pdb.set_trace()
        pv_table_keys = list(pv_table.keys())
        chip_keys = list(chip.keys())
        # pdb.set_trace()
        probb = sell_prob(pv_table_keys, chip_keys, ratio, date)

        pv_table_values = list(pv_table.values())


        #to_pct = turnover_ratio * 100
        #to_wegt = {'0': 0.9, '1': }
        #if turnover_ratio <  :    
        #    weight = 0.1
        #else:
        #    weigtt = turnover_ratio 
        weight = 1 - turnover_ratio


        probb_1 = [(1-weight) * probb[i] + weight * pv_table_values[i] for i in range(0, len(probb))]    # weight和换手率正相关，对应的应该是随机部分的比例，因为有了收益的期望，所以会导致换手率降低

        for i in range(0, len(pv_table)):
            key = list(pv_table.keys())[i]
            # pv_table[key] = (1 - turnover_ratio * probb[i]) * pv_table[key]   # 这里可以改进，筹码并不是真得按照换手率等比例分布的，应该是越接近该价格的越容易卖出，越远离该价格的不容易卖出
            pv_table[key] = pv_table[key] - turnover_ratio * probb_1[i]

        # for key, value in pv_table.items():
        #     pv_table[key] = (1 - turnover_ratio) * value  # 指南针的方法 等比例调整现有筹码，昨天的筹码分布图value先乘以今天的换手率，等比例缩减。

        for key, value in chip.items():
            if key not in pv_table.keys():  # 新价格，对应的新的筹码直接就是当天分价表中的筹码
                pv_table[key] = value
            else:
                pv_table[key] = pv_table[key] + value  # 今天成交的筹码加上之前剩下的筹码

        ### 处理pv_tables中某些value为负数的情况
        market_cap = turnover_volume / turnover_ratio
        threshold = 0.00001  # 阈值定义为100股对应的比例，也就是占比少于100股的价格都归零
        pv_table_adj = {}
        for key, value in pv_table.items():
            if value > threshold:
                pv_table_adj[key] = value
        # print(date,  sum(list(pv_table_adj.values())))
        sum_value = sum(list(pv_table_adj.values()))
        for key, value in pv_table_adj.items():  # 等比率调整
            pv_table_adj[key] = value / sum_value

        pv_table = {}
        for key, value in pv_table_adj.items():
            pv_table[str(round(float(key), 2))] = str("%.8f"%float(value))
        

        return pv_table, close_price
        # print(date, pv_table_adj)
        # else:
        #     # 第一天 筹码表还没有建立
        #     pv_table = chip
        #     pv_table[ipo_price] = 1 - turnover_ratio  # 未成交的筹码都是原始发行价

        # ope_mysql.insert(self.date, pv_table_adj)  # 每天都写入数据库中,会有停盘无数据的情况
        # print(self.code, self.date, "over")
    except Exception as e:
        pdb.set_trace()
        print("Exception: ", str(e))
        print(date, code, "无数据")
        s=sys.exc_info()
        print ("Error '%s' happened on line %d" % (s[1],s[2].tb_lineno))

        #pdb.set_trace()
    

def new_write_onestock(item, date):
    code_table = {'6': "pricetable_zb", '0': "pricetable_zxb", '3': "pricetable_cyb"}
    code_name = str(item)[1:len(str(item))]  # 1600000 -> "600000"
    if code_name == "601313" and int(date) < 20180228:    # code_name 是需要去检查是否存在该股票，所以是601360  item是从pricetable中来，pricetable中在20180228之前都是601313
        #pdb.set_trace()
        code_name = "601360"

    if code_name[0] == '6':        # 有501050这样的基金，也有000300这样的指数
        tmp_code = code_name + '.SH'  # 600000.SH
        int_indexcode = int('1' + code_name)
    elif code_name[0] == '0' or code_name[0] == '3':
        tmp_code = code_name + '.SZ'
        int_indexcode = int('2' + code_name)
    else:
        tmp_code = ''
    #if int_indexcode == 1601360:  # sum_df中用的是601313
    #    int_indexcode = 1601313

    # if str(iitem)[1] in ["6","0","2"]:     # 有些是500打头的
#    if tmp_code in list(code_list):  # 有些是000300 沪深300
    if int_indexcode == 1601360 and int(date) < 20180228:  # sum_df中用的是601313  因为20180228之后才更名为601360
        int_indexcode = 1601313

    # 开始表演   处理数据
    # 先查询是否存在股票记录，不存就就是新股
    
    #sum_df = pd.read_csv("/data/yue_ming_pricetable/pricetable/" + item)
    
    #conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_backup", port=3306, charset='utf8')
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_backup", port=3306, charset='utf8')
    cur = conn.cursor()
    pricetable = code_table[code_name[0]]  # 根据code_table  dict获得是那一张表
    sql = "select count(*) from %s where code='%s'" % (pricetable, code_name)
    cur.execute(sql)
    #db.cursor.execute(sql)
    #row = db.cursor.fetchone()
    row = cur.fetchone()        

    if row[0] == 0:
        # 刚上市的新股，需要取ipo价格来进行计算
        #ipo_price = initial_info.loc[tmp_code]["首发价格"]  # 发行价
        #if ipo_price != ipo_price:  # 说明ipo价格为nan, 此时设置ipo_price 为1
        #    ipo_price = 1
        tmp_ddf = sum_df.loc[sum_df['SecurityID'] == int_indexcode]  # 直接从sum_df中切片索引得到该股票的ddf
        ddf = tmp_ddf.loc[tmp_ddf['Price'] != 0]
        ipo_price = min(ddf["Price"])/10000/1.2
        initial_pvtable = {ipo_price: 1}
        today_pvtable, close_price = cal_pvtable(initial_pvtable, ddf, date, code_name)

    else:
        # 已经存在的股票，取最近的chip开始计算即可
        #sql = "select chip from %s where code='%s' order by tra_date desc limit 1" % (pricetable, code_name)
        sql = "select chip from %s where code='%s' and length(chip)>4 order by tra_date desc limit 1" % (pricetable, code_name)
        # cur.execute("select chip from %s where code='%s' order by tra_date desc limit 1"%(pricetable, code_name))
        # cur.execute("select chip from %s where code='%s' order by tra_date desc limit 1 ")%(pricetable, code_name) 注意这种错误写法  写在了括号外面
        cur.execute(sql)
        row = cur.fetchone()
        #db.cursor.execute(sql)
        #row = db.cursor.fetchone()
        try:
            yesterday_pvtable = eval(row[0])
        except Exception as e:
            print("Error")
            #pdb.set_trace()
            yesterday_pvtable = {}
        #yesterday_pvtable = eval(row[0])
        tmp_ddf = sum_df.loc[sum_df['SecurityID'] == int_indexcode]  # 直接从sum_df中切片索引得到该股票的ddf
        ddf = tmp_ddf.loc[tmp_ddf['Price'] != 0]  # 月明计算的合成表中存在价格为0的记录，也就是Falg = 4
        today_pvtable, close_price = cal_pvtable(yesterday_pvtable, ddf, date, code_name)

    #pdb.set_trace()
#  增加收盘价的读取，因此在单独计算支撑压力位的过程中，发现读取有困难，所以在写入20160104之后的筹码的过程中便写入收盘价、支撑位和压力位
#    file_name = {'6':'.SH.CSV', '0':".SZ.CSV", '3':".SZ.CSV"}
#    df = pd.read_csv("/data/write_mysql_20180325/re_write/front_exclude_close/" + code_name + file_name[code_name[0]], index_col=2, encoding="gbk")
#    try:
#        close_price = df.loc[int(date),'收盘价(元)']    # 存量数据读取close的时候是从wind数据库中导出的,wind数据中导出的已经是前复权收盘价了
#    except Exception as e:
#        close_price = 0.0

## 计算出支撑压力位
    sp_price_dict = extreme(today_pvtable, close_price)    # 需要获得前复权价格   002668 NoneType has no attribute 'item'

#    records.append((float(sp_price_dict["P"]),float(sp_price_dict["S"]), code, item[0]))


    return code_name, today_pvtable, close_price, sp_price_dict['P'],sp_price_dict['S'] 
    #return item+1,date

    # records.append((code_name, date, str(today_pvtable)))   # 名称，日期，筹码  把单个pricetable中的所有股票都记录在一个list中，然后一次写入

def cal_or_not(item, sum_df, date):

    if str(item)[0] == '1':
        if str(item)[1] != '6':
            return False
    if str(item)[0] == '2':
        if str(item)[1] not in ['0','3']:
            return False
    if str(item)[0:2] == '20':
        if str(item)[2] != '0':
            return False
    if str(item)[0:2] == '23':
        if str(item)[2] != '0':
            return False


    code_name = str(item)[1:len(str(item))]  # 1600000 -> "600000"
#    if code_name == "601313":    # code_name 是需要去检查是否存在该股票，所以是601360
#        code_name = "601360"
    
#    if code_name[0] == '6':        # 有501050这样的基金，也有000300这样的指数
#        tmp_code = code_name + '.SH'  # 600000.SH
#    elif code_name[0] == '0' or code_name[0] == '3':
#        tmp_code = code_name + '.SZ'
#    else:
#        tmp_code = ''   


    #if tmp_code in list(code_list):  # 有些是000300 沪深300
        #dddf = pd.read_csv("/root/project_price/vol_turnover_test_ex_factor/" + tmp_code + ".CSV", encoding='gbk',index_col=3)
    #    dddf = pd.read_csv("/root/project_price/vol_turnover_test_ex_factor_20180404/" + tmp_code + ".CSV", encoding='gbk',index_col=0)
    #    if dddf.loc[int(date), "成交量(股)"] != '--':   # 存在成交量，说明当天该股票存在交易，wind导出的数据是最准确的。月明读取的数据有错误
    tmp_ddf = sum_df.loc[sum_df['SecurityID'] == item]  # 直接从sum_df中切片索引得到该股票的ddf
    ddf = tmp_ddf.loc[tmp_ddf['Price'] != 0]
    if not ddf.empty:    # ddf不为空才说明当天真实没有停牌
        if min(ddf["Price"]) / 10000 < 1000:     # 指数 1000016   以及个股2000016 二者只有通过价格来区分，指数价格高于1000
            if item != 2000916:    # 000916华北高速换股成招商公路
                return True
   
 
    return False


#    以下是增量计算的代码，对比以下，修改存量计算的cal_or_not的代码
#    tmp_ddf = sum_df.loc[sum_df['SecurityID'] == item]  # 直接从sum_df中切片索引得到该股票的ddf
#    ddf = tmp_ddf.loc[tmp_ddf['Price'] != 0]
#    if not ddf.empty:  # ddf不为空才说明当天真实没有停牌
#        if min(ddf["Price"]) / 10000 < 1000:     # 指数 1000016   以及个股2000016 二者只有通过价格来区分，指数价格高于1000
#            return True

#    return False


            

def new_write_oneday_pricetable(sum_df, date):
    records_zb = []
    records_zxb = []
    records_cyb = []

    results = []
    pool = multiprocessing.Pool(processes=16)

    for item in set(sum_df["SecurityID"]):  # 代码集合
            #pdb.set_trace()
        if str(item)[1] in ['0', '3', '6']:   # 有些是基金，目前发现的基金代码以5开头
            if cal_or_not(item,sum_df, date):   # 是股票代码且sum_df中不全是0，也就是当天没有停牌
                # write_oneday_pricetable(iitem, row_list_tables, date, sum_df, initial_info)
                result = pool.apply_async(new_write_onestock, args=(item, date))
                #result = new_write_onestock(item, date)
                results.append(result)
    pool.close()
    pool.join()

    # code_name, today_pvtable, close_price, sp_price_dict['S'],sp_price_dict['P']

    #pdb.set_trace()
    for result in results:
        code_name = result.get()[0]
        if code_name[0] == '6':
            records_zb.append((code_name, date, str(result.get()[1]), str(result.get()[2]), str(result.get()[3]), str(result.get()[4])))    # result.get()   # 返回的pv_table
        if code_name[0] == '0':
            records_zxb.append((code_name, date, str(result.get()[1]), str(result.get()[2]), str(result.get()[3]), str(result.get()[4])))
        if code_name[0] == '3':
            records_cyb.append((code_name, date, str(result.get()[1]), str(result.get()[2]), str(result.get()[3]), str(result.get()[4])))
 
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_backup", port=3306, charset='utf8')
    cur = conn.cursor()
    
    #pdb.set_trace()
     
    try:
        cur.executemany("replace into pricetable_zb (code, tra_date, chip, close, pre_p, sup_p) values(%s,%s,%s,%s,%s,%s)", records_zb)
        conn.commit()
    except Exception as e:
        conn.rollback()

    try:
        cur.executemany("replace into pricetable_zxb (code, tra_date, chip, close, pre_p, sup_p) values(%s,%s,%s,%s,%s,%s)", records_zxb)
        conn.commit()
    except Exception as e:
        conn.rollback()

    try: 
        cur.executemany("replace into pricetable_cyb (code, tra_date, chip, close, pre_p, sup_p) values(%s,%s,%s,%s,%s,%s)", records_cyb)
        conn.commit()
    except Exception as e:    
        conn.rollback()
    
    cur.close()
    conn.close()
    print(date, 'over')
#    except Exception as e:
#        conn.rollback()
#        print(date, "Exception: ", str(e))
#     for result in results:
#        code_name = result.get()[0]
#        if code_name[0] == '6':
#            records_zb.append((code_name, date, str(result.get()[1]), str(result.get()[2]), str(result.get()[3]), str(result.get()[4])))    # result.get()   # 返回的pv_table
#        if code_name[0] == '0':
#            records_zxb.append((code_name, date, str(result.get()[1]), str(result.get(2)), str(result.get(3)), str(result.get(4))))
#        if code_name[0] == '3':
#            records_cyb.append((code_name, date, str(result.get()[1]), str(result.get(2)), str(result.get(3)), str(result.get(4))))
    

#    with getPTConnection() as db:    
#        try:
#            db.cursor.executemany("insert into pricetable_zb (code, tra_date, chip, close, pre_p, sup_p) values(%s,%s,%s,%s,%s,%s)", records_zb)
#            db.cursor.executemany("insert into pricetable_zxb (code, tra_date, chip, close, pre_p, sup_p) values(%s,%s,%s,%s,%s,%s)", records_zxb)
#            db.cursor.executemany("insert into pricetable_cyb (code, tra_date, chip, close, pre_p, sup_p) values(%s,%s,%s,%s,%s,%s)", records_cyb)

#            db.conn.commit()
#            print(date, " over")
#        except Exception as e:
#            db.conn.rollback()
#            print(date, "Exception: ", str(e))
    


if __name__ == '__main__':
    t1 = time.time()
    #initial_info = pd.read_csv("/root/project_price/initial_info.csv", encoding='gbk', index_col=0)  # 获得所有股票的ipo时间
    #code_list = list(pd.read_csv("/data/write_mysql_20180325/code_list.csv",encoding='gbk')["股票代码"])   # 所有股票的代码，Tick数据里面由一些是基金  譬如510050  000300

    files_name = []
    for (root, dirs, files) in os.walk("/data/yue_ming_pricetable/pricetable"):
        for item in files:
            files_name.append(item)
    files_name = sorted(files_name)  # files_name 读出的信息是乱序的
        

#    with getPTConnection() as db:    
    for item in files_name:          # 一个pricetable是一个循环，一次计算完一个pricetable
        print(item)
        date = item[0:8]  # 20160104
        sum_df = pd.read_csv("/data/yue_ming_pricetable/pricetable/" + item)
        # 修改为一天只处理一个连接
        #conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,
        #                       db='pv_table', charset='utf8')
#        conn=pymysql.connect(host='127.0.0.1', user='root', passwd='', db="pv_table", port=3306, charset='utf8')
#        cur = conn.cursor()
#        sql_get_all_tables = "select table_name from information_schema.TABLES where TABLE_SCHEMA='pv_table'"
#        cur.execute(sql_get_all_tables)
#        row_list_tables = cur.fetchall()
#        if int(date) < 20180207:
        new_write_oneday_pricetable(sum_df, date)   
 
               
                


#from DB_connetion_pool import getPTConnection, PTConnectionPool;
#
#def TestMySQL():
#    #申请资源  
#    with getPTConnection() as db:
#        # SQL 查询语句;
#        sql = "SELECT tra_date FROM pricetable_zb where code='600000'";
#        try:
#            # 获取所有记录列表
#            db.cursor.execute(sql)
#            results = db.cursor.fetchall();
#            for row in results:
#                print(row[0])
#                # 打印结果
#        except:
#            print ("Error: unable to fecth data")
#
#TestMySQL()






#        with ProcessPoolExecutor(4) as executor:
#            for iitem in list(set(sum_df["SecurityID"]))[0:1]:  # 代码集合
#                # write_oneday_pricetable(iitem, row_list_tables, date, sum_df, initial_info)
#                code_name = str(iitem)[1:len(str(iitem))]  # 1600000 -> "600000"
#                if code_name == "601313":
#                    code_name = "601360"
#                if code_name[0] == '6':
#                    tmp_code = code_name + '.SH'  # 600000.SH
#                elif code_name[0] == '0' or code_name[0] == '3':
#                    tmp_code = code_name + '.SZ'
#                
#                pdb.set_trace()                
#
#                if str(iitem)[1] in ["6","0","2"]:
#                    if tmp_code in list(code_list):
#                        future = executor.submit(write_oneday_pricetable, iitem, row_list_tables, date, sum_df, initial_info)
        
        # 多线程走起
        ##      在这里考虑用用多线程
        #with ProcessPoolExecutor(4) as executor:
        #    for iitem in list(set(sum_df["SecurityID"])):  # 代码集合
                # write_oneday_pricetable(iitem, row_list_tables, date, sum_df, initial_info)
        #        executor.submit(write_oneday_pricetable, iitem, row_list_tables, date, sum_df, initial_info)
            # code_name = str(iitem)[1:len(str(iitem))]    # 1600000 -> "600000"
            # ddf = sum_df.loc[sum_df['SecurityID'] == iitem]   #   直接从sum_df中切片索引得到该股票的ddf
            # if len(ddf):   # 如果存在记录
            #     #pool.apply_async(write_pv_table, (code_name, date, ddf))
            #     write_pv_table(code_name, date, ddf)
        # pool.close()
        # pool.join()


        # for iitem in set(sum_df["SecurityID"]):  # 代码集合
        #     code_name = str(iitem)[1:len(str(iitem))]  # 1600000 -> "600000"
        #     # 由ddf计算pv_table
        #     if code_name[0] == '6':
        #         table_name = 'sh' + code_name   # sh600000
        #         tmp_code = code_name + '.SH'    # 600000.SH
        #     elif code_name[0] == '0' or code_name[0] == '3':
        #         table_name = 'sz' + code_name
        #         tmp_code = code_name + '.SZ'
        #
        #     #   ("sh600000",) in row
        #     if (table_name, ) in row_list_tables:   # 有该股票名字的表
        #         # 需要注意停牌的情况
        #         last_id_sql = "SELECT * FROM %s ORDER BY id DESC LIMIT 1"%(table_name)
        #         cur.execute(last_id_sql)
        #         row = cur.fetchall()
        #         try:
        #             if str(row[0][1]).replace("-", "") == date:  # str(row[0][1]) == "20161124"
        #                 print(date, table_name, " exists")
        #             else:
        #                 ddf = sum_df.loc[sum_df['SecurityID'] == iitem]  # 直接从sum_df中切片索引得到该股票的ddf
        #                 today_pvtable = cal_pvtable(eval(row[0][2]), ddf, date, code_name)
        #                 sql_insert_pvtable = "insert into %s (Tra_Date,Chip) values('%s','%s')" % (
        #                     table_name, date, today_pvtable)
        #                 cur.execute(sql_insert_pvtable)
        #                 conn.commit()
        #                 print(date, table_name, " cal done")
        #         except Exception as e:
        #             print(str(e), row)
        #     else:                       # 新股票
        #         try:
        #             ipo_price = initial_info.loc[tmp_code]["首发价格"]  # 发行价
        #             if ipo_price != ipo_price:  # 说明ipo价格为nan, 此时设置ipo_price 为1
        #                 ipo_price = 1
        #             initial_pvtable = {ipo_price: 1}
        #             ddf = sum_df.loc[sum_df['SecurityID'] == iitem]  # 直接从sum_df中切片索引得到该股票的ddf
        #             today_pvtable = cal_pvtable(initial_pvtable, ddf, date, code_name)
        #             sql_insert_pvtable = "insert into %s (Tra_Date,Chip) values('%s','%s')" % (
        #                 table_name, date, today_pvtable)
        #             cur.execute(sql_insert_pvtable)
        #             conn.commit()
        #             print(date, table_name, " cal done")
        #         except Exception as e:
        #             print(str(e), date, code_name)


            # sql_select_pv_table = "select * from %s where Tra_Date ='%s'" % (table_name, date)
            # cur.execute(sql_select_pv_table)     # 查询当天分价表是否存在
            #
            # row = cur.fetchall()
            # if not len(row):  # date日期的分价表不存在，开始计算
            #     ddf = sum_df.loc[sum_df['SecurityID'] == iitem]  # 直接从sum_df中切片索引得到该股票的ddf
            #
            #     # 取前一交易日分价表，开始计算
            #     # 现在已知该股票当天的ddf，也就是当天的分价表，还需要获得前一天分价表，再计算，然后再写入到Mysql当中去
            #
            #     # date -> yesterday   self.date[0:4] + '-' + self.date[4:6] + '-' + self.date[6:8]
            #     index = list_tra_date.index([date[0:4] + '-' + date[4:6] + '-' + date[6:8], 1])  # date肯定为交易日
            #     for i in range(index - 1, -1, -1):
            #         if list_tra_date[i][-1] == 1:
            #             yesterday = list_tra_date[i][0]
            #             break
            #
            #     # 取得昨天的pv_table
            #     yesterday_pv_table = "select * from %s where Tra_Date ='%s'" % (table_name, yesterday.replace("-", ""))
            #     cur.execute(yesterday_pv_table)
            #     row_yesterday = cur.fetchall()
            #
            #     if row_yesterday:
            #         # 可以利用yesterday和今天的分价表来计算筹码表
            #         today_pvtable = cal_pvtable(eval(row_yesterday[0][2]), ddf, date, code_name)
            #         sql_insert_pvtable = "insert into %s (Tra_Date,Chip) values('%s','%s')" % (
            #         table_name, date, today_pvtable)
            #         cur.execute(sql_insert_pvtable)
            #         conn.commit()
            #     else:  # yesterday 不存在，则说明是第一天
            #         if code_name[0] == '6':
            #             tmp_code = code_name + '.SH'
            #         else:
            #             tmp_code = code_name + ".SZ"
            #         ipo_price = initial_info.loc[tmp_code]["首发价格"]  # 发行价
            #         if ipo_price != ipo_price:  # 说明ipo价格为nan, 此时设置ipo_price 为1
            #             ipo_price = 1
            #         initial_pvtable = {ipo_price: 1}
            #
            #         today_pvtable = cal_pvtable(initial_pvtable, ddf, date)
            #         sql_insert_pvtable = "insert into %s (Tra_Date,Chip) values('%s','%s')" % (
            #         table_name, date, today_pvtable)
            #         cur.execute(sql_insert_pvtable)
            #         conn.commit()
            #
            # else:     # date日期的分价表存在  应该不做任何事，继续下一个循环
            #     print(date, table_name, " exists")

        # sum_df = pd.read_csv("E:/wind_export_files/yue_ming_pricetable/pricetable/" + item)  # 20160104.csv
        #
        # # pool = multiprocessing.Pool(processes = 4)
        # with ThreadPoolExecutor(32) as executor:
        #     # with ProcessPoolExecutor(16) as executor:
        #     for iitem in set(sum_df["SecurityID"]):  # 代码集合
        #         # if str(iitem) == "2002778":
        #         executor.submit(cal_each_stock_in_one_pricetable, iitem, sum_df, date)
        #     # code_name = str(iitem)[1:len(str(iitem))]    # 1600000 -> "600000"
        #     # ddf = sum_df.loc[sum_df['SecurityID'] == iitem]   #   直接从sum_df中切片索引得到该股票的ddf
        #     # if len(ddf):   # 如果存在记录
        #     #     #pool.apply_async(write_pv_table, (code_name, date, ddf))
        #     #     write_pv_table(code_name, date, ddf)
        # # pool.close()
        # # pool.join()
    t2 = time.time()
    print(t2 - t1)

# with ThreadPoolExecutor(8) as executor:
#     for each in files_name[0][9:]:
#         executor.submit(write_mysql, each)

