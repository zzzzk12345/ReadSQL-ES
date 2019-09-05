import mysql.connector

"""

edited by zhangkai 2019/5/23

"""

# 连接数据库
mydb = mysql.connector.connect(
    host="192.168.251.232",
    user="zhangkai",
    passwd="zhangkai",
    database="bizopswh"
)

mycursor = mydb.cursor()

mycursor.execute("SELECT * FROM passenger_histories LIMIT 50")  # 选择全部表中数据

tb_title = []
des = mycursor.description
for i in des:
    tb_title.append(i[0])
print(tb_title)
"""
[('id', 3, None, None, None, None, 0, 49699), 
('dianxiao_price', 5, None, None, None, None, 1, 32768), 
('dianxiao_block', 253, None, None, None, None, 1, 0), 
('dianxiao_latest_time', 253, None, None, None, None, 1, 0), 
('call_times', 8, None, None, None, None, 1, 32768), 
...
]
"""

# rows = mycursor.fetchmany(20)
# iterator = iter(rows)
# a = next(iterator)
# print(a)
# print(next(iterator))


