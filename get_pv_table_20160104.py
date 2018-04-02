import json
import urllib.request
import pandas as pd
import datetime
import pymysql
import multiprocessing
import time
import os
from numba import jit

class GetPVTable(object):
    def __init__(self, code, date, ddf):
        self.code = code
        self.date = date
        self.ddf = ddf
    @jit
    def sell_prob(self, pv_table_keys, chip_keys, ratio, date):
        prob = []
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

        earning_negative_2 = [j - sum(earning_negative_1) for j in earning_negative_1]   # 当只有一个负值时，此时earning_negative_2会变成[0.0]

        if earning_positive and len(earning_negative_2) != 1:  # earning_positive 为空说明全是负的收益，则不用乘以系数
            prob_negative = [(1 - ratio) * j / sum(earning_negative_2) for j in earning_negative_2]
        elif len(earning_negative_2) == 1:  # len(earning_negative_2) == 1 说明只有一个负值，此时earning_negative_2 = [0.0]，无法作为除数
            prob_negative = [1-ratio]
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

    def cal_pv_table(self):     # 计算筹码分布图，最终返回一个dict
        
        ope_mysql = OpeMysql(self.code)    # 初始化时就创建了数据库
        if self.code[0] == '6':
            tmp_code = self.code + '.SH'
        else:
            tmp_code = self.code + ".SZ"     # tmp_code = lambda x: x + '.SH' if x[0] == '6' else x + '.SZ'
#             #  lambda 和if else的联合使用，注意lambda x 相当于def func(x)
#         datetime_ipo_date = datetime.datetime.strptime(initial_info.loc[tmp_code]["上市日期"], '%Y/%m/%d')
#
#         pv_table = {}   # 初始分价表
#         ipo_date = initial_info.loc[tmp_code]["上市日期"]   # 上市日期
#         ipo_price = initial_info.loc[tmp_code]["首发价格"]  # 发行价
#         #print(datetime_ipo_date > datetime.datetime.strptime("2016/01/01", '%Y/%m/%d'))
#         if datetime_ipo_date > datetime.datetime.strptime("2016/01/01", '%Y/%m/%d'):    # 在2016年1月1号之后上市的新股
#             if ipo_price != ipo_price:  # 说明ipo价格为nan, 此时设置ipo_price 为1
#                 ipo_price = 1
#             #index = list_tra_date.index([ipo_date, 1])
#             pv_table[ipo_price] = 1
#             #else:
#             #    pv_table[ipo_price] = 1
#         else:  # 在2016年1月1号之前上市的新股  pv_table用映射成的数据
# #            with open('./pv_table_json_files/' + self.code + '.json') as json_file:
# #                pv_table_str = json.load(json_file)
# #            for key, value in pv_table_str.items():     # json 文件load进来的key是str类型
# #                pv_table[float(key)] = valu      # 最后一天的筹码分布截止到20151231e
#             pv_table = ope_mysql.get("20151231")      # 最后一天的筹码分布截止到20151231
#             index = list_tra_date.index(["2016-01-04", 1])
#
#         ipo_date = ipo_date.replace("/", "-")
#         tmp = ipo_date.split("-")    # 变2016/1/6为2016/01/06
#         if len(tmp[1]) == 1:
#             tmp[1] = '0' + tmp[1]
#         if len(tmp[2]) == 1:
#             tmp[2] = '0' + tmp[2]
#         ipo_date = tmp[0] + "-" + tmp[1] + "-" + tmp[2]
#         index = list_tra_date.index([ipo_date, 1])
         dddf = pd.read_csv("./vol_turnover_test/" + tmp_code + ".CSV", encoding='gbk', index_col=3)    # 读取包含换手率、成交量和复权因子的CSV文件

        # 现在已知code date 和ddf  计算pv_table
        # 第一步 先查看数据库中是否存在该数据库，如果不存在，说明股票是第一天上市，需要获得ipo_price，如果存在，则直接读取其前一日的pv_table

        index = list_tra_date.index([self.date, 1])   # date肯定为交易日
        for i in range(index, -1, -1):
            if list_tra_date[i][-1] == 2:
                yesterday = list_tra_date[i][0]

        # yesterday = list_tra_date[index - 1][0]   #获取前一天
        pv_table = ope_mysql.get(yesterday)

        if not pv_table:     # 如果pv_table为空，说明此时还没有建立筹码分布图，说明这只股票今天是其上市首日
            ipo_price = initial_info.loc[tmp_code]["首发价格"]  # 发行价
            if ipo_price != ipo_price:  # 说明ipo价格为nan, 此时设置ipo_price 为1
                ipo_price = 1
            pv_table[ipo_price] = 1

        # for item in list_tra_date[index:]:
        #     if item[-1] != 2:  # 2代表休市
        #         date = item[0].replace("-", '')  # trade date

        code_12 = '1' + self.code if self.code[0] == '6' else '2' + self.code
        try:
        #   ddf = sum_df.loc[sum_df['SecurityID'] == int(code_12)]   #   直接从sum_df中切片索引得到ddf

            cum_factor = dddf.loc[int(date), "复权因子"]
            turnover_ratio = dddf.loc[int(date), "换手率(%)"] / 100         # 换手率
            turnover_volume = dddf.loc[int(date), "成交量(股)"]             # 成交量

            price = list(ddf["Price"]/10000)
            volume = list(turnover_ratio * ddf["Volume"] / turnover_volume)
            chip = dict(zip(price,volume))        #直接形成chip表
            ratio = ddf["Will"].sum()/ddf["TotalTx"].sum()

            # if pv_table:  # 根据分价表更新当天的筹码分布图
            # 分价表的数据是无关乎除权复权的，因此需要先检查筹码分布表
            tmp = {v: k for k, v in pv_table.items()}  # pv_table 和value 反过来
            ttmp = {k: v / cum_factor for k, v in tmp.items()}  # 调整除权
            pv_table = {round(v, 2): k for k, v in ttmp.items()}  # 再反过来
            probb = self.sell_prob(list(pv_table.keys()), list(chip.keys()), ratio, date)

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
            threshold = 500 / market_cap    # 阈值定义为100股对应的比例，也就是占比少于100股的价格都归零
            pv_table_adj = {}
            for key, value in pv_table.items():
                if value > threshold:
                    pv_table_adj[key] = value
            #print(date,  sum(list(pv_table_adj.values())))
            sum_value = sum(list(pv_table_adj.values()))
            for key, value in pv_table_adj.items():             # 等比率调整
                pv_table_adj[key] = value / sum_value

            pv_table = pv_table_adj
            #print(date, pv_table_adj)
            # else:
            #     # 第一天 筹码表还没有建立
            #     pv_table = chip
            #     pv_table[ipo_price] = 1 - turnover_ratio  # 未成交的筹码都是原始发行价

            #ope_mysql.insert(date, pv_table_adj)    # 每天都写入数据库中,会有停盘无数据的情况
            print(pv_table)
            print(date, "over")
        except Exception as e:
            print("Exception: ", str(e))
            #print(date, code_12, "无数据")
                
        return pv_table

    def cal_s_p_price(self):    # 计算支撑压力位，最终返回一个dict
        pass

    def write_mysql(self):
        self.cal_pv_table()


    def plot(self):             # 作图函数，画出筹码分布图
        #print(json.dumps(self.cal_pv_table(), indent=2))
        pass


class OpeMysql(object):    # 每只股票都会有一个操作数据库的累
    def __init__(self, code):
        if code[0] == '6':
            self.database_name = 'sh' + code
        elif code[0] == '0' or code[0] == '3':
            self.database_name = 'sz' + code

        self.create(self.database_name)
#        conn = pymysql.connect(host='0.0.0.0', user='root', passwd='passw0rd', port=3306, charset='utf8')
#        cur = conn.cursor()
#        cur.execute("show databases")
#        tuple_databases = cur.fetchall()
#        db_name_lists = []
#
#        for item in tuple_databases:
#            db_name_lists.append(item[0])     # get all databases;
#        if self.database_name not in db_name_lists:
#            self.create(self.database_name)

    def create(self, db_name):    # 专门创建某只股票的数据库
        conn = pymysql.connect(host='0.0.0.0', user='root', passwd='passw0rd', port=3306, charset='utf8')
        sql = "create database if exists " + db_name
        cur = conn.cursor()
        cur.execute(sql)     # 创建数据库
        sql = "use " + db_name
        cur.execute(sql)

        sql_cre_pv_table = '''
            create table pv_table(
                Tra_Date date not null,
                Chip text not null
            )
        '''
        cur.execute(sql_cre_pv_table)     # 创建分价表table
        conn.close()


    def destroy(self, db_name):
        conn = pymysql.connect(host='0.0.0.0', user='root', passwd='passw0rd', port=3306, charset='utf8')
        sql = "drop database if exists " + db_name
        cur = conn.cursor()
        cur.execute(sql)  # 删除数据库
        conn.close()

    def insert(self, date, pv_table):    # 将某天的分价表插入到数据库当中
        conn = pymysql.connect(host='0.0.0.0', user='root', passwd='passw0rd', port=3306, charset='utf8')
        cur = conn.cursor()
        cur.execute("use " + self.database_name)
        sql_insert_pv_table = "insert into pv_table(Tra_Date,Chip) values('%s','%s')" % (date, pv_table)
        cur.execute(sql_insert_pv_table)
        conn.close()

    def get(self, date):     # 读取某天的分价表
        conn = pymysql.connect(host='0.0.0.0', user='root', passwd='passw0rd', port=3306, charset='utf8')
        cur = conn.cursor()
        try:
            cur.execute("use " + self.database_name)
            sql_select_pv_table = "select * from pv_table where Tra_Date ='%s'" % (date)
            cur.execute(sql_select_pv_table)
            row = cur.fetchall()    # 获取执行结果 
            conn.close()
            #if len(row):  # 当天的记录存在
            return eval(row[0][1])    # 返回dict类型的当天的筹码分布图
        except Exception as e: 
            return {}    

def write_pv_table(code, date, ddf):   # 函数里产生类，函数用于多线程 
    test = GetPVTable(code, date, ddf)    
    test.write_mysql()

#def get_pv_table(code):
#    test = GetPVTable(code)
#    test.get('20171229')


if __name__ == '__main__':
    url_tra_date = "http://fintech.jrj.com.cn/tp/astock/getholidays?start=1990-12-01&end=2018-03-14"
    res_tra_date = urllib.request.urlopen(url_tra_date)
    html_tra_date = res_tra_date.read()
    list_tra_date = json.loads(html_tra_date.decode('utf-8'))['data']
    t1 = time.time() 
    initial_info = pd.read_csv("./initial_info.csv", encoding='gbk', index_col=0)   # 获得所有股票的ipo时间
    
    #pool = multiprocessing.Pool(processes = 50)
    #for k in range(len(initial_info)):
    #for k in range(1):
    #for k in range(2513, 2514):
    #    if initial_info.index[k][0] == "3":
            #print(initial_info.index[k][0:6])
    #        pool.apply_async(write_pv_table, (initial_info.index[k][0:6], ))
    #pool.close()
    #pool.join()
    files_name = []
    for (root, dirs, files) in os.walk("/data/yue_ming_pricetable/"):
        for item in files:
            if len(item) == 11: 
                files_name.append(item)   

    for item in files_name[0]
        date = item[0:8]     # 20160104 
        sum_df = pd.read_csv("/data/yue_ming_pricetable/" + item)   # 20160104.csv

        pool = multiprocessing.Pool(processes = 4)
        for iitem in set(sum_df["SecurityID"][0:3]):    # 代码集合
            code_name = str(iitem)[1:len(str(iitem))]    # 1600000 -> "600000"
            ddf = sum_df.loc[sum_df['SecurityID'] == iitem   #   直接从sum_df中切片索引得到该股票的ddf
            print(code_name)
            pool.apply_async(write_pv_table, (code_name, date, ddf))
        pool.close()
        pool.join()
    t2 = time.time()
    print(t2-t1)

