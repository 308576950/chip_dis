import pymysql


def add_other_fields(table_name):   # 增加close_price字段
    sql_add_field = "alter table %s add total_number int, add supbit_number int, add presbit_number int, add propct_cerprc mediumtext, add profit_pct float, add cost_avg float, add score float"%(table_name)
    #conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,
    #                       db='pv_table', charset='utf8')
    #conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_backup", port=3306, charset='utf8')
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table", port=3306, charset='utf8')
    cur = conn.cursor()
    try:
        cur.execute(sql_add_field)
        conn.commit()
        print("add fields success")
    except Exception as e:
        print("Exception: ", str(e))
        conn.rollback()

def add_score_field(table_name):   # 增加close_price字段
    sql_add_field = "alter table %s add score float"%(table_name)
    #conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,
    #                       db='pv_table', charset='utf8')
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_backup", port=3306, charset='utf8')
    cur = conn.cursor()
    try:
        cur.execute(sql_add_field)
        conn.commit()
        print("add fields success")
    except Exception as e:
        print("Exception: ", str(e))
        conn.rollback()


def add_chip_classify_field(table_name):   # 增加close_price字段
    sql_add_field = "alter table %s add chip_classify int"%(table_name)
    #conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,
    #                       db='pv_table', charset='utf8')
    #conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_backup", port=3306, charset='utf8')
    #conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_dba", port=3306, charset='utf8')
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table", port=3306, charset='utf8')
    cur = conn.cursor()
    try:
        cur.execute(sql_add_field)
        conn.commit()
        print("add fields success")
    except Exception as e:
        print("Exception: ", str(e))
        conn.rollback()


if __name__=="__main__":
    #conn = pymysql.connect(host='172.16.20.103', user='JRJ_pv_table', passwd='9JEhCpbeu3YXxyNpFDVQ', port=3308,db='pv_table', charset='utf8')
#    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='passw0rd', db="pv_table_backup", port=3306, charset='utf8')
#    cur = conn.cursor()
#    sql_get_all_tables = "select table_name from information_schema.TABLES where TABLE_SCHEMA='pv_table'"
#    cur.execute(sql_get_all_tables)
    row_list_tables = ["pricetable_zb", "pricetable_zxb", "pricetable_cyb"]
    for item in row_list_tables:
        add_chip_classify_field(item)
#        add_other_fields(item)
#         add_score_field(item)
#        print(item[0])
#        delete_duplicate(item[0])
#        unique_Tra_Date(item[0])
#        print(item[0]," unique Tra_Date")
#        add_close_field(item[0])
#    print(len(row_list_tables))
