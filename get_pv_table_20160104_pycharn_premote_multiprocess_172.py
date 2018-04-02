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

filterwarnings('ignore', category=pymysql.Warning)

#@jit
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
    dddf = pd.read_csv("/root/project_price/vol_turnover_test_ex_factor/" + tmp_code + ".CSV", encoding='gbk',
                               index_col=3)  # 读取包含换手率、成交量和复权因子的CSV文件
    ## 需要计算出从date日期到现在时间的累积复权因子
    cum_factor = reduce(lambda x, y: x * y, dddf.loc[int(date)+1:, "复权因子"])    # 从该天之后的累计复权因子，用来将Tick数据中的真实价格转化成相对于此时此刻的前复权价格     

    #pdb.set_trace()
    try:
        ex_factor = dddf.loc[int(date), "复权因子"]
        turnover_ratio = float(dddf.loc[int(date), "换手率(%)"]) / 100  # 换手率
        turnover_volume = float(dddf.loc[int(date), "成交量(股)"])  # 成交量

        # market_cap = turnover_volume / turnover_ratio

        pv_table = {}   # 传进来的tmp_pv_table的key是str类型
        for key, value in tmp_pv_table.items():
            pv_table[float(key)] = value


        price = list(ddf["Price"] / 10000 / cum_factor)     # 除以cum_factor, 比如从20160104之后只分红过两次，一次是16年6月17，一次是17年7月16，则cum_factor 等于这两次的除权因子的乘积。参加300299.
        volume = [i * turnover_ratio / turnover_volume for i in list(ddf["Volume"])]
        chip = dict(zip(price, volume))  # 直接形成chip表
        ratio = ddf["Will"].sum() / ddf["TotalTx"].sum()
        # if pv_table:  # 根据分价表更新当天的筹码分布图
        # 分价表的数据是无关乎除权复权的，因此需要先检查筹码分布表
        #if ex_factor != 1:
        #    tmp = {v: k for k, v in pv_table.items()}  # pv_table 和value 反过来
        #    ttmp = {k: v / ex_factor for k, v in tmp.items()}  # 调整除权
        #    pv_table = {round(v, 2): k for k, v in ttmp.items()}  # 再反过来

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
        print("Exception: ", str(e))
        # print(date, code_12, "无数据")


#
# def cal_each_stock_in_one_pricetable(iitem, sum_df, date):
#     code_name = str(iitem)[1:len(str(iitem))]  # 1600000 -> "600000"
#     ddf = sum_df.loc[sum_df['SecurityID'] == iitem]  # 直接从sum_df中切片索引得到该股票的ddf
#     if len(ddf):  # 如果存在记录
#         # pool.apply_async(write_pv_table, (code_name, date, ddf))
#         write_pv_table(code_name, date, ddf)


def write_oneday_pricetable(iitem, row_list_tables, date, sum_df, initial_info):
    code_name = str(iitem)[1:len(str(iitem))]  # 1600000 -> "600000"
    if code_name == "601313":
        code_name = "601360"
    # 由ddf计算pv_table
    #if code_name[0] in ['6','0','2']
    if code_name[0] == '6':
        table_name = 'sh' + code_name  # sh600000
        tmp_code = code_name + '.SH'  # 600000.SH
    elif code_name[0] == '0' or code_name[0] == '3':
        table_name = 'sz' + code_name
        tmp_code = code_name + '.SZ'

#    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='', db="pv_table", port=3306, charset='utf8')
    conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308, db='pv_table', charset='utf8')
    cur = conn.cursor()

    #   ("sh600000",) in row
    if (table_name,) in row_list_tables:  # 有该股票名字的表
        # 需要注意停牌的情况
        #last_id_sql = "SELECT * FROM %s ORDER BY id DESC LIMIT 1" % (table_name)

        last_id_sql = "select * from %s where Chip != '' order by id desc limit 1" % (table_name)

    #    pdb.set_trace()
        cur.execute(last_id_sql)
        row = cur.fetchall()
        #try: 
        #    print(str(row[0][1]).replace("-", ""))
        #except Exception as e:
        #    print(row, str(e))
        #    pdb.set_trace()        

        if str(row[0][1]).replace("-", "") == date:  # str(row[0][1]) == "20161124"
            print(date, table_name, " exists")
        else:
           
            ddf = sum_df.loc[sum_df['SecurityID'] == iitem]  # 直接从sum_df中切片索引得到该股票的ddf
            #pdb.set_trace()
            today_pvtable = cal_pvtable(eval(row[0][2]), ddf, date, code_name)
            #sql_insert_pvtable = "insert into %s (Tra_Date,Chip,) values('%s','%s')" % (
            #    table_name, date, today_pvtable)
            #pdb.set_trace()
            sql_insert_pvtable = "insert into %s (Tra_Date,Chip, pre_p, sup_p) values('%s','%s',0.0, 0.0)" % (table_name, date, today_pvtable)
            try:
                cur.execute(sql_insert_pvtable)
                conn.commit()
                print(date, table_name, " cal done")
            except Exception as e:
                conn.rollback()
                print(str(e), "老股", "计算失败")

    else:  # 新股票   对于新股票，需要先建立table
       
        ipo_price = initial_info.loc[tmp_code]["首发价格"]  # 发行价
        if ipo_price != ipo_price:  # 说明ipo价格为nan, 此时设置ipo_price 为1
            ipo_price = 1
        initial_pvtable = {ipo_price: 1}
        ddf = sum_df.loc[sum_df['SecurityID'] == iitem]  # 直接从sum_df中切片索引得到该股票的ddf

            # 先创建table
            #sql_cre_table = "create table if not exists %s (id int auto_increment primary key,Tra_Date date not null,Chip text not null)" % table_name
        sql_cre_table = "create table if not exists %s(id int auto_increment primary key,Tra_Date date not null,Chip mediumtext not null, pre_p double, sup_p double)" % table_name            
        try:
            cur.execute(sql_cre_table)  # 创建分价表table
            conn.commit()
            today_pvtable = cal_pvtable(initial_pvtable, ddf, date, code_name)
            sql_insert_pvtable = "insert into %s (Tra_Date,Chip) values('%s','%s')" % (
                table_name, date, today_pvtable)
            cur.execute(sql_insert_pvtable)
            conn.commit()
            print(date, table_name, " cal done")
        except Exception as e:
            conn.rollback()
            print(str(e), "新股", " 计算失败")


if __name__ == '__main__':
    url_tra_date = "http://fintech.jrj.com.cn/tp/astock/getholidays?start=1990-12-01&end=2018-03-14"
    res_tra_date = urllib.request.urlopen(url_tra_date)
    html_tra_date = res_tra_date.read()
    list_tra_date = json.loads(html_tra_date.decode('utf-8'))['data']
    t1 = time.time()
    initial_info = pd.read_csv("/root/project_price/initial_info.csv", encoding='gbk', index_col=0)  # 获得所有股票的ipo时间
    code_list = pd.read_csv("/data/write_mysql_20180325/code_list.csv",encoding='gbk')["股票代码"]   # 所有股票的代码，Tick数据里面由一些是基金  譬如510050  000300

    files_name = []
    for (root, dirs, files) in os.walk("/data/yue_ming_pricetable/pricetable"):
        for item in files:
            files_name.append(item)
    files_name = sorted(files_name)  # files_name 读出的信息是乱序的
    for item in files_name[0:1]:          # 一个pricetable是一个循环，一次计算完一个pricetable
        print(item)
        date = item[0:8]  # 20160104
        sum_df = pd.read_csv("/data/yue_ming_pricetable/pricetable/" + item)
        # 修改为一天只处理一个连接
        #conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,
        #                       db='pv_table', charset='utf8')
        #conn=pymysql.connect(host='127.0.0.1', user='root', passwd='', db="pv_table", port=3306, charset='utf8')
        conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308, db='pv_table', charset='utf8')
        cur = conn.cursor()
        sql_get_all_tables = "select table_name from information_schema.TABLES where TABLE_SCHEMA='pv_table'"
        cur.execute(sql_get_all_tables)
        row_list_tables = cur.fetchall()

        futures = set()
        with ProcessPoolExecutor(4) as executor:
            for iitem in list(set(sum_df["SecurityID"])):  # 代码集合
                # write_oneday_pricetable(iitem, row_list_tables, date, sum_df, initial_info)

                code_name = str(iitem)[1:len(str(iitem))]  # 1600000 -> "600000"
                if code_name == "601313":
                    code_name = "601360"
                if code_name[0] == '6':
                    tmp_code = code_name + '.SH'  # 600000.SH
                elif code_name[0] == '0' or code_name[0] == '3':
                    tmp_code = code_name + '.SZ'
                
                if str(iitem)[1] in ["6","0","2"]:
                    if tmp_code in list(code_list):
                        future = executor.submit(write_oneday_pricetable, iitem, row_list_tables, date, sum_df, initial_info)
                        futures.add(future)
        try:
            for future in as_completed(futures):
                err = future.exception()
                if err is not None:
                    raise err
        except KeyboardInterrupt:
                print("stopped by hand")
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

