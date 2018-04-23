import pymysql
import pdb
from warnings import filterwarnings

filterwarnings('ignore', category=pymysql.Warning)

if __name__ == '__main__':
    conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308, db='pv_table', charset='utf8')
    cur = conn.cursor()
    #cur.execute("select distinct(tra_date) from pricetable_cyb_20180418 order by tra_date desc limit 10")
    cur.execute("select distinct(tra_date) from pricetable_cyb order by tra_date desc limit 30")
    row = cur.fetchall()

    conn1 = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', port=3306, db='pv_table', charset='utf8')
    cur1 = conn1.cursor()
    cur1.execute("select distinct(tra_date) from pricetable_cyb order by tra_date desc limit 30")
    row1 = cur1.fetchall()
   
    for item in row1:
        if item not in row:
            # 堡垒机中存在而dba中不存在
           tra_date = str(item[0]).replace('-','')
           cur1.execute("select * from pricetable_cyb where tra_date=%s"%tra_date)
           results = []
           for item in cur1.fetchall():
               results.append(item)
           #cur.executemany("insert into pricetable_cyb_20180418 values (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", results)             
           try:
               cur.executemany("insert into pricetable_cyb values (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", results)             
           except Exception as e:
               print(str(e))
               conn.roolback()

