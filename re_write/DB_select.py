#-*- coding: UTF-8 -*-  
'''
@描述：
@作者：CYH
@版本：V1.0
@创建时间：2016-11-24 上午9:34:54
'''
from DB_connetion_pool import getPTConnection, PTConnectionPool;

def TestMySQL():  
    #申请资源  
    with getPTConnection() as db:
        # SQL 查询语句;
        sql = "SELECT tra_date FROM pricetable_zb where code='600000'";
        try:
            # 获取所有记录列表
            db.cursor.execute(sql)
            results = db.cursor.fetchall();
            for row in results:
                print(row[0])
                # 打印结果
        except:
            print ("Error: unable to fecth data")

TestMySQL()
