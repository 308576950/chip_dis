# 本文是用来撰写每天更新的脚本
# 思路  每天定时读取月明的表， 保留上一次的文件名称，看看有没有更新的数值， 如果有，则更新。
# 更新需要的数据，收盘价、 pricetable，复权信息、

import urllib.request
from bs4 import BeautifulSoup
import os
import pymysql
import pandas as pd
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
from DB_connetion_pool_pv_table_back_up import getPTConnection, PTConnectionPool;
import numpy as np
from heapq import nlargest
import tushare as ts

filterwarnings('ignore', category=pymysql.Warning)

def cal_href(item):
    if len(item) == 9:    # 20160104/
        file_name = item
        url = "http://jobs.fintech.lugu/level2/ana/" + item + "/pricetable.csv"
#        rresponse = urllib.request.urlopen(url)
#        hhtml = rresponse.read()
        shell_order = "wget -O "+ './pricetable/' + item[0:-1] + "_pricetable.csv " + url
        os.system(shell_order)
        print(item[0:-1], "over")
#        with open("E:/pv_table_file/" + date + "/tmp/" + item[0:7] + "_tmp.csv", 'wb') as f:
#            f.write(hhtml)
#
#        ddf = pd.read_csv("E:/pv_table_file/" + date + "/tmp/" + item[0:7] + "_tmp.csv")[["Price", "Volume", "Flag"]]
#        chip, ratio = cal_ddf(ddf)
#        df = pd.DataFrame(list(chip.items()), columns=['price', 'volumes'])   # 整理成的dict转化成dataframe
#        df["ratio"] = ratio
#        df.to_csv("E:/pv_table_file/" + date + "/" + file_name, encoding="utf-8")
#        print(file_name, " over")


# 读取月明的网页中的pricetable的所有日期，决定是否需要更新  返回['20160104', '20160105', '20160106']
def get_pricetable():
    response = urllib.request.urlopen("http://jobs.fintech.lugu/level2/ana/")
    html = response.read()
    soup = BeautifulSoup(html, "lxml")
    href_list = soup.find_all("a")

    href_llist= []
    for item in href_list:
        href_llist.append(item.contents[0])

    href_llist = [item[0:8] for item in href_llist if len(item) == 9]

    return href_llist
    # 每天定时读取到href_llist的日期变量。 下一步进行根据href_llist更新筹码表


# 计算支撑压力位
def extreme(my_dict, close):  # 思路是在收盘价的一个涨跌幅之内最大的筹码密集区
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

        my_array1 = np.arange(round(0.9 * close, 2), close, 0.01)  # 低于收盘价，计算支撑位
        my_array2 = np.arange(close, round(1.1 * close, 2), 0.01)  # 高于收盘价，计算压力位

        tmp1 = []
        tmp2 = []

        for i in my_array1:
            if round(i, 2) in list(my_dict.keys()):
                tmp1.append({"price": i, "chip": my_dict[round(i, 2)]})  # 找出支撑位的筹码备选区间
        if nlargest(1, tmp1, key=lambda s: s["chip"]):
            s_p_price['S'] = round(nlargest(1, tmp1, key=lambda s: s["chip"])[0]["price"], 2)
        else:
            s_p_price['S'] = 0

        for i in my_array2:  # 找出压力位的筹码备选区间
            if round(i, 2) in list(my_dict.keys()):
                tmp2.append({"price": i, "chip": my_dict[round(i, 2)]})
        if nlargest(1, tmp2, key=lambda s: s["chip"]):
            s_p_price['P'] = round(nlargest(1, tmp2, key=lambda s: s["chip"])[0]["price"], 2)
        else:
            s_p_price['P'] = 0
        return s_p_price  # {"S":支撑位, "P":压力位}

#计算卖出概率数组
def sell_prob(pv_table_keys, chip_keys, ratio, date):
    prob = []
    # pdb.set_trace()
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


# 由昨天的筹码分布 tmp_pv_table计算今天的筹码分布，需要利用较多的中间数据
def cal_pvtable(tmp_pv_table, ddf, date, code):  # 利用昨天筹码图，当天分价表和date来计算date
    if code[0] == '6':
        tmp_code = code + '.SH'
    else:
        tmp_code = code + ".SZ"  # tmp_code = lambda x: x + '.SH' if x[0] == '6' else x + '.SZ'
    #dddf = pd.read_csv("/root/project_price/vol_turnover_test_ex_factor/" + tmp_code + ".CSV", encoding='gbk',
    #                   index_col=3)  # 读取包含换手率、成交量和复权因子的CSV文件
    ## 需要计算出从date日期到现在时间的累积复权因子
    # try:
    #    cum_factor = reduce(lambda x, y: x * y, dddf.loc[int(date)+1:, "复权因子"])    # 从该天之后的累计复权因子，用来将Tick数据中的真实价格转化成相对于此时此刻的前复权价格
    # except Exception as e:
    #    cum_factor = 1
    #    print(str(e), code, " no cum_factor")

    # pdb.set_trace()
    try:
        # 直接上tushare  df = ts.get_hist_data('600000',start='2016-01-06',end='2016-01-06')
        date_ = date[0:4] + '-' + date[4:6] + '-' + date[6:]
        df = ts.get_hist_data(code, start=date_, end=date_)
        turnover_ratio = df.loc[date_, "turnover"] / 100
        turnover_volume = df.loc[date_, "volume"] * 100

        # ex_factor = dddf.loc[int(date), "复权因子"]
        url = "http://fintech.jrj.com.cn/tp/astock/getfactor?code=%s&date=%s" % (code, date_)
        res = urllib.request.urlopen(url)
        html = res.read()
        try:
            ex_date = json.loads(html.decode('utf-8'))['data'][0]['ex_date']
            if ex_date == date_:
                ex_factor = json.loads(html.decode('utf-8'))['data'][0]['ex_factor']    #这里不对 不应该用累计复权因子，应该用单次因子
            else:
                ex_factor = 1
        except Exception as e:
            ex_factor = 1
            print("Exception: ", str(e))

        pv_table = {}  # 传进来的tmp_pv_table的key是str类型
        for key, value in tmp_pv_table.items():
            pv_table[float(key)] = value

        price = list(ddf["Price"] / 10000)  # 除以cum_factor, 比如从20160104之后只分红过两次，一次是16年6月17，一次是17年7月16，则cum_factor 等于这两次的除权因子的乘积。参加300299.
        volume = [i * turnover_ratio / turnover_volume for i in list(ddf["Volume"])]
        chip = dict(zip(price, volume))  # 直接形成chip表
        # if ddf["TotalTx"].sum() == 0:
        #    ratio =
        ratio = ddf["Will"].sum() / ddf["TotalTx"].sum()
        # if pv_table:  # 根据分价表更新当天的筹码分布图                  注意  现价永远是最大的
        # 分价表的数据是无关乎除权复权的，因此需要先检查筹码分布表
        if ex_factor != 1:
            tmp = {v: k for k, v in pv_table.items()}  # pv_table 和value 反过来
            ttmp = {k: v / ex_factor for k, v in tmp.items()}  # 调整除权
            pv_table = {round(v, 2): k for k, v in ttmp.items()}  # 再反过来

        #    pdb.set_trace()
        pv_table_keys = list(pv_table.keys())
        chip_keys = list(chip.keys())
        # pdb.set_trace()
        probb = sell_prob(pv_table_keys, chip_keys, ratio, date)

        pv_table_values = list(pv_table.values())
        probb_1 = [0.5 * probb[i] + 0.5 * pv_table_values[i] for i in range(0, len(probb))]

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

        return pv_table_adj
        # print(date, pv_table_adj)
        # else:
        #     # 第一天 筹码表还没有建立
        #     pv_table = chip
        #     pv_table[ipo_price] = 1 - turnover_ratio  # 未成交的筹码都是原始发行价

        # ope_mysql.insert(self.date, pv_table_adj)  # 每天都写入数据库中,会有停盘无数据的情况
        # print(self.code, self.date, "over")
    except Exception as e:
        pdb.set_trace()
        print("Exception: ", str(e))      # 'NoneType' object has no attribute 'loc'
        print(date, code, "无数据", tmp_pv_table)
        # pdb.set_trace()

# 写某只股票的函数
def new_write_onestock(item, date):
    code_table = {'6': "pricetable_zb", '0': "pricetable_zxb", '3': "pricetable_cyb"}
    code_name = str(item)[1:len(str(item))]  # 1600000 -> "600000"
    if code_name == "601313":  # code_name 是需要去检查是否存在该股票，所以是601360
        # pdb.set_trace()
        code_name = "601360"

    if code_name[0] == '6':  # 有501050这样的基金，也有000300这样的指数
        tmp_code = code_name + '.SH'  # 600000.SH
        int_indexcode = int('1' + code_name)
    elif code_name[0] == '0' or code_name[0] == '3':
        tmp_code = code_name + '.SZ'
        int_indexcode = int('2' + code_name)
    else:
        tmp_code = ''
    # if int_indexcode == 1601360:  # sum_df中用的是601313
    #    int_indexcode = 1601313

    # if str(iitem)[1] in ["6","0","2"]:     # 有些是500打头的
    #    if tmp_code in list(code_list):  # 有些是000300 沪深300
    if int_indexcode == 1601360:  # sum_df中用的是601313
        int_indexcode = 1601313

    # 开始表演   处理数据
    # 先查询是否存在股票记录，不存就就是新股

    # sum_df = pd.read_csv("/data/yue_ming_pricetable/pricetable/" + item)

    # conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_backup", port=3306, charset='utf8')
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table", port=3306, charset='utf8')
    cur = conn.cursor()
    pricetable = code_table[code_name[0]]  # 根据code_table  dict获得是那一张表
    sql = "select count(*) from %s where code='%s'" % (pricetable, code_name)
    cur.execute(sql)
    # db.cursor.execute(sql)
    # row = db.cursor.fetchone()
    row = cur.fetchone()

    if row[0] == 0:

        tmp_ddf = sum_df.loc[sum_df['SecurityID'] == int_indexcode]  # 直接从sum_df中切片索引得到该股票的ddf
        ddf = tmp_ddf.loc[tmp_ddf['Price'] != 0]
        ipo_price = min(ddf["Price"])/10000/1.2   # 新股上市首日的价格，发行价是开盘价除以1.2

        # 刚上市的新股，需要取ipo价格来进行计算
        #ipo_price = initial_info.loc[tmp_code]["首发价格"]  # 发行价   发行价用最低价除以1.2即可得到
        #if ipo_price != ipo_price:  # 说明ipo价格为nan, 此时设置ipo_price 为1
        #    ipo_price = 1
        initial_pvtable = {ipo_price: 1}

        today_pvtable = cal_pvtable(initial_pvtable, ddf, date, code_name)

    else:
        # 已经存在的股票，取最近的chip开始计算即可
        # sql = "select chip from %s where code='%s' order by tra_date desc limit 1" % (pricetable, code_name)
        sql = "select chip from %s where code='%s' and length(chip)>4 order by tra_date desc limit 1" % (
        pricetable, code_name)
        # cur.execute("select chip from %s where code='%s' order by tra_date desc limit 1"%(pricetable, code_name))
        # cur.execute("select chip from %s where code='%s' order by tra_date desc limit 1 ")%(pricetable, code_name) 注意这种错误写法  写在了括号外面
        cur.execute(sql)
        row = cur.fetchone()
        # db.cursor.execute(sql)
        # row = db.cursor.fetchone()
        try:
            yesterday_pvtable = eval(row[0])
        except Exception as e:
            print("Error")
            # pdb.set_trace()
        yesterday_pvtable = eval(row[0])
        tmp_ddf = sum_df.loc[sum_df['SecurityID'] == int_indexcode]  # 直接从sum_df中切片索引得到该股票的ddf
        ddf = tmp_ddf.loc[tmp_ddf['Price'] != 0]  # 月明计算的合成表中存在价格为0的记录，也就是Falg = 4
        today_pvtable = cal_pvtable(yesterday_pvtable, ddf, date, code_name)

    # pdb.set_trace()
    #  增加收盘价的读取，因此在单独计算支撑压力位的过程中，发现读取有困难，所以在写入20160104之后的筹码的过程中便写入收盘价、支撑位和压力位
    # file_name = {'6': '.SH.CSV', '0': ".SZ.CSV", '3': ".SZ.CSV"}
    # df = pd.read_csv("/data/write_mysql_20180325/re_write/front_exclude_close/" + code_name + file_name[code_name[0]],
    #                  index_col=2, encoding="gbk")
    date_ = date[0:4] + '-' + date[4:6] + '-' + date[6:]
    try:
        url = 'http://fintech.jrj.com.cn/tp/astock/dayhis?code=%s&start=%s&end=%s&type=new'%(code_name, date_, date_)
        res = urllib.request.urlopen(url)
        html = res.read()
        code_today_hangqing = json.loads(html.decode('utf-8'))
        close_price = code_today_hangqing['data'][0]['tclose']    # 从fintech.jrj中获取close price
    except Exception as e:
        close_price = 0.0
        print("Exception: ", str(e))

    ## 计算出支撑压力位
    sp_price_dict = extreme(today_pvtable, close_price)  # 需要获得前复权价格   002668 NoneType has no attribute 'item'

    #    records.append((float(sp_price_dict["P"]),float(sp_price_dict["S"]), code, item[0]))

    return code_name, today_pvtable, close_price, sp_price_dict['S'], sp_price_dict['P']
    # return item+1,date

    # records.append((code_name, date, str(today_pvtable)))   # 名称，日期，筹码  把单个pricetable中的所有股票都记录在一个list中，然后一次写入


# 决定该代码是否需要写入的函数，因为有的代码可能是基金或者指数
def cal_or_not(item, sum_df, date, tmp_dict):

    code_name = str(item)[1:len(str(item))]  # 1600000 -> "600000"

    # if code_name[0] == '6':  # 有501050这样的基金，也有000300这样的指数
    #     tmp_code = code_name + '.SH'  # 600000.SH
    # elif code_name[0] == '0' or code_name[0] == '3':
    #     tmp_code = code_name + '.SZ'
    # else:
    #     tmp_code = ''
    if code_name in tmp_dict['data'].keys():   # 需要是在当天股票列表中
        tmp_ddf = sum_df.loc[sum_df['SecurityID'] == item]  # 直接从sum_df中切片索引得到该股票的ddf
        ddf = tmp_ddf.loc[tmp_ddf['Price'] != 0]
        if not ddf.empty:  # ddf不为空才说明当天真实没有停牌
            return True
    else:
        return False


    # if tmp_code in list(code_list):  # 有些是000300 沪深300
    #     dddf = pd.read_csv("/root/project_price/vol_turnover_test_ex_factor/" + tmp_code + ".CSV", encoding='gbk',
    #                        index_col=3)
    #     if dddf.loc[int(date), "成交量(股)"] != '--':  # 存在成交量，说明当天该股票存在交易，wind导出的数据是最准确的。月明读取的数据有错误
    #         tmp_ddf = sum_df.loc[sum_df['SecurityID'] == item]  # 直接从sum_df中切片索引得到该股票的ddf
    #         ddf = tmp_ddf.loc[tmp_ddf['Price'] != 0]
    #         if not ddf.empty:  # ddf不为空才说明当天真实没有停牌
    #             return True
    # else:
    #     return False



# 写某天的pricetable的函数
def new_write_oneday_pricetable(sum_df, date):
    url = "http://fintech.jrj.com.cn/tp/astock/getallfactor?date=" + date[0:4] + '-' + date[4:6] + '-' + date[6:] #20180403  --> 2018-04-03
    res = urllib.request.urlopen(url)
    html = res.read()
    tmp_dict = json.loads(html.decode('utf-8'))   # 获取所有当天股票代码


    records_zb = []
    records_zxb = []
    records_cyb = []

    results = []
    pool = multiprocessing.Pool(processes=8)

    for item in set(sum_df["SecurityID"]):  # 代码集合
        if str(item)[1] not in ['0', '1', '2']:
            if cal_or_not(item, sum_df, date, tmp_dict):  # 是股票代码且sum_df中不全是0，也就是当天没有停牌
                #write_oneday_pricetable(iitem, row_list_tables, date, sum_df, initial_info)
            #result = pool.apply_async(new_write_onestock, args=(item, date))
                result = new_write_onestock(item, date)
                results.append(result)
    #pool.close()
    #pool.join()

    # code_name, today_pvtable, close_price, sp_price_dict['S'],sp_price_dict['P']

    # pdb.set_trace()
    for result in results:
        code_name = result.get()[0]
        if code_name[0] == '6':
            records_zb.append((code_name, date, str(result.get()[1]), str(result.get()[2]), str(result.get()[3]),
                               str(result.get()[4])))  # result.get()   # 返回的pv_table
        if code_name[0] == '0':
            records_zxb.append(
                (code_name, date, str(result.get()[1]), str(result.get(2)), str(result.get(3)), str(result.get(4))))
        if code_name[0] == '3':
            records_cyb.append(
                (code_name, date, str(result.get()[1]), str(result.get(2)), str(result.get(3)), str(result.get(4))))

    with getPTConnection() as db:
        try:
            db.cursor.executemany(
                "insert into pricetable_zb (code, tra_date, chip, close, pre_p, sup_p) values(%s,%s,%s,%s,%s,%s)",
                records_zb)
            db.cursor.executemany(
                "insert into pricetable_zxb (code, tra_date, chip, close, pre_p, sup_p) values(%s,%s,%s,%s,%s,%s)",
                records_zxb)
            db.cursor.executemany(
                "insert into pricetable_cyb (code, tra_date, chip, close, pre_p, sup_p) values(%s,%s,%s,%s,%s,%s)",
                records_cyb)

            db.conn.commit()
            print(date, " over")
        except Exception as e:
            db.conn.rollback()
            print(date, "Exception: ", str(e))



if __name__ == '__main__':
    pricetabl_dates = get_pricetable()    # ['20160104', '20160105', '20180206']
    # select tra_date from pricetable_zb order by tra_date desc limit 1
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table", port=3306,
                           charset='utf8')
#    cur = conn.cursor()
#    cur.execute("select tra_date from pricetable_cyb order by tra_date desc limit 1")   # 查找最后一天
#    row = cur.fetchone()[0]
#    last_day = str(row).replace('-', '')    # '2016-01-04' -->  20160104

#    index = pricetabl_dates.index(last_day)

    sum_df = pd.read_csv("/data/yue_ming_pricetable/pricetable/20180207_pricetable.csv")    # 读取下载的CSV
    new_write_oneday_pricetable(sum_df, '20180207')
    #for i in range(index + 1, len(pricetabl_dates)):
#    for i in range(index + 1, index + 2):
#        # 请开始你的表演
#        url = "http://jobs.fintech.lugu/level2/ana/" + pricetabl_dates[i] + "/pricetable.csv"
#        shell_order = "wget -O " + '/data/yue_ming_pricetable/pricetable/' + pricetabl_dates[i] + "_pricetable.csv" + url
#        os.system(shell_order)    # 下载
#
#        sum_df = pd.read_csv('/data/yue_ming_pricetable/pricetable/' + pricetabl_dates[i] + "_pricetable.csv")    # 读取下载的CSV
#        # 下一步开始计算该sum_df  难点在于没有中间数据
#        # 一步一步梳理中间数据
#        new_write_oneday_pricetable(sum_df, pricetabl_dates[i])
#



# if __name__ == '__main__':
#     t1 = time.time()
#     initial_info = pd.read_csv("/root/project_price/initial_info.csv", encoding='gbk', index_col=0)  # 获得所有股票的ipo时间
#     code_list = list(pd.read_csv("/data/write_mysql_20180325/code_list.csv", encoding='gbk')[
#                          "股票代码"])  # 所有股票的代码，Tick数据里面由一些是基金  譬如510050  000300
#
#     files_name = []
#     for (root, dirs, files) in os.walk("/data/yue_ming_pricetable/pricetable"):
#         for item in files:
#             files_name.append(item)
#     files_name = sorted(files_name)  # files_name 读出的信息是乱序的
#
#     #    with getPTConnection() as db:
#     for item in files_name[9:]:  # 一个pricetable是一个循环，一次计算完一个pricetable
#         print(item)
#         date = item[0:8]  # 20160104
#         sum_df = pd.read_csv("/data/yue_ming_pricetable/pricetable/" + item)
#         # 修改为一天只处理一个连接
#         # conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,
#         #                       db='pv_table', charset='utf8')
#         #        conn=pymysql.connect(host='127.0.0.1', user='root', passwd='', db="pv_table", port=3306, charset='utf8')
#         #        cur = conn.cursor()
#         #        sql_get_all_tables = "select table_name from information_schema.TABLES where TABLE_SCHEMA='pv_table'"
#         #        cur.execute(sql_get_all_tables)
#         #        row_list_tables = cur.fetchall()
#         new_write_oneday_pricetable(sum_df, date)
#
#     t2 = time.time()
#     print(t2 - t1)





