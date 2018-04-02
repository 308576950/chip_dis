import pymysql


def add_close_field(table_name):   # 增加close_price字段
    sql_add_field = "alter table %s add close float"%(table_name)
    #conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,
    #                       db='pv_table', charset='utf8')
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='', db="pv_table", port=3306, charset='utf8')
    cur = conn.cursor()
    try:
        cur.execute(sql_add_field)
    except Exception as e:
        print("Exception: ", str(e))
        conn.rollback()


def unique_Tra_Date(table_name):
    sql_unique_Tra_Date = "alter table %s add unique (Tra_Date)"%(table_name)
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='', db="pv_table", port=3306, charset='utf8')
    cur = conn.cursor()
    try:
        cur.execute(sql_unique_Tra_Date)
    except Exception as e:
        print("Exception: ", str(e))
        conn.rollback()


def delete_duplicate(table_name):
    sql_delete_duplicate = "delete from %s where id not in (select * from (select id from %s group by Tra_Date) As b)"%(table_name, table_name)
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='', db="pv_table", port=3306, charset='utf8')
    cur = conn.cursor()
    try:
        cur.execute(sql_delete_duplicate)
    except Exception as e:
        print("Exception: ", str(e))
        conn.rollback()



if __name__=="__main__":
    #conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,db='pv_table', charset='utf8')
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='', db="pv_table", port=3306, charset='utf8')
    cur = conn.cursor()
    sql_get_all_tables = "select table_name from information_schema.TABLES where TABLE_SCHEMA='pv_table'"
    cur.execute(sql_get_all_tables)
    row_list_tables = cur.fetchall()    ## 类似格式 (("sh600000",),("sz000001", ))
    for item in row_list_tables:
#        print(item[0])
        delete_duplicate(item[0])
#        unique_Tra_Date(item[0])
        print(item[0]," unique Tra_Date")
#        add_close_field(item[0])
#    print(len(row_list_tables))
