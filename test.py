#!/usr/bin/python3.6.5
# -*- coding: utf-8 -*-
# 读取excel中存放的专家信息，存储到mysql数据库（专家库），用户通过运行程序以交互的方式构建抽取规则，然后从专家库中随机抽取专家，程序再将抽取结果存放到mysql数据库中
#
# 环境要求：
# 1，需要mysql，设置好localhost（user, password, database），启动mysql:
# https://www.jianshu.com/p/07a9826898c0，本程序默认密码为123123123
# 2，需要安装以下模块：xlrd, pymysql, treelib

import xlrd
import pymysql
import sys
import treelib
from treelib import Node, Tree

# 打开excel文件函数，获取专家信息


def open_excel():
	# noinspection PyBroadException
	try:
		book = xlrd.open_workbook(r'./namelist.xlsx')  # 打开xls文件
	except:
		print('Open excel file failed')
	# noinspection PyBroadException
	try:
		get_sheet = book.sheet_by_name('namelist')
		return get_sheet
	except:
		print('Locate worksheet in excel failed')

# 将专家信息存入数据库


def insert_data(data_source, table_name):
	# sheet = open_excel()
	# cursor = db.cursor()

	row_num = data_source.nrows
	a = 'truncate %s' % (table_name)
	cursor.execute(a)  # 清空原来库中的专家名单

	for i in range(1, row_num):  # 第一行是标题名，对应表中的字段名所以应该从第二行开始，计算机以0开始计数，所以值是1
		row_data = data_source.row_values(i)
		value = (row_data[0], row_data[1], row_data[2], row_data[3], row_data[4], row_data[5], row_data[6], row_data[7], row_data[8], row_data[9])
		# print(value)
		sql = "INSERT INTO namelist(spec_id,name,profession,dept2,dept3,title,category,hiredate,source,contact)VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % value
		# print(sql)
		cursor.execute(sql)  # 执行SQL语句
		dele_null_data = "delete from %s where spec_id = '';" % table_name  # 删除空行
		cursor.execute(dele_null_data)
		db.commit()
	# cursor.close()  # 关闭指针


# 排除重复数据


def eliminate_duplicate_data(one_result, table_eliminate_from):
	print('开始一次查重程序')
	print('待查重的记录为：', one_result)
	search_query = 'select * from %s where locate(%s, spec_id);' % (table_eliminate_from, one_result[1])
	print('在%s中查找spec_id=%s' % (table_eliminate_from, one_result[1]))
	print(search_query)
	r = cursor.execute(search_query)
	print('查重执行返回值r=', r)
	search_result = cursor.fetchall()
	print('查重结果search_result=', search_result)
	if r > 0:
		print('一次查重结束。已抽取过此位专家，重新抽取...')
		return 1
	elif r == 0:
		print('一次查重结束。未抽取过此专家，可以抽取')
		return -1


# 随机抽取专家的函数


def random_select(table_select_from, category, extract_number):
	print('随机抽取一组')
	# cursor = db.cursor()
	random_id = 'spec_index >= round(' \
				'(' \
				'(select max(spec_index) from %s) - (select min(spec_index) from %s)' \
				')*rand()' \
				') + (select min(spec_index) from %s)' % (table_select_from, table_select_from,table_select_from)
	query = 'SELECT * FROM namelist WHERE %s and category = "%s" LIMIT %d;' % (random_id, category, extract_number)  # 按照指定条件随机抽取专家
	print(query)
	# 执行SQL语句
	cursor.execute(query)
	results = cursor.fetchall()
	print('此次目标是抽取%d位专家，此次实际共抽取到%d位专家。分别如下：' % (extract_number, len(results)))
	print(results)
	# print('已执行查询语句')
	# 获取所有记录列表，打印
	print('一组随机抽取结束！')
	return results


def result_insert(results_to_insert, table_insert_to, category, table_select_from):
	print('开始存入抽取结果...')
	add_result_list = []
	for aa in range(0, len(results_to_insert)):
		add_result_list.append(results_to_insert[aa][1])

#  检查结果中是否有和之前抽取的重复
	for old_i in range(0, len(results_to_insert)):
			# print('开始执行循环')
		sign = eliminate_duplicate_data(results_to_insert[old_i], table_insert_to)
		print('员工编号=%s，原记录中查重结果sign为%s\n' % (results_to_insert[old_i][1], sign))
		loop_count = 0
		#  用列表记录重新抽取的结果，避免重新抽取的虽然不和数据库已有的重复，但是和之前重新抽取的结果重复

		while sign == 1 and loop_count < 500:
			print('sign=1，有重复结果')
			add_result = random_select(table_select_from, category, 1)
			print('重新抽取的结果为：', add_result)

			#  判断重新抽取的结果是否和库里已有的结果重复
			sign_add_result = eliminate_duplicate_data(add_result[0], table_insert_to)

			#  判断重新抽取的结果是否和之前重新抽取的重复
			if sign_add_result != 1 and add_result[0][1] not in add_result_list:
				#  因为要插入的结果为元组类型不能直接修改，需要先修改为list类型才能更新
				temp = list(results_to_insert)
				temp[old_i] = add_result[0]
				results_to_insert = tuple(temp)
				print('完成一次重复结果的替换')

				add_result_list.append(add_result[0][1])
				print('add_result_list列表增加一条记录：', add_result[0][1])
				print('目前的add_result_list=', add_result_list)
				sign = eliminate_duplicate_data(results_to_insert[old_i], table_insert_to)
				print('此时sign变为：', sign)
			elif sign == -1:
				print('结果已经不重复')
			else:
				print('重新抽取的结果和之前重新抽取的重复，再重新抽取')

			loop_count += 1
			print('loop_count=',loop_count)
			if loop_count >= 500:
				print('可用专家不足，中止抽取')

		print('继续查重')

	print('全部查重结束，开始保存数据...')
	try:
		for new_i in range(0, len(results_to_insert)):
			sql = "insert into %s(spec_index, spec_id, name, profession, dept2, dept3, title, category, hiredate, source, contact)" % table_insert_to + "values(%d,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % results_to_insert[new_i]
			# print(sql)
			cursor.execute(sql)
			print(results_to_insert[new_i])
			db.commit()
	except:
		print("Error: unable to insert data")
	print('抽取结果完成保存！')
	# cursor.close()  # 关闭指针

# 构造随机抽取专家的字典


def input_extract_tree(table_select_from, table_insert_to):

	choice = str(input('是否清空之前保存的抽取结果？请输入Y或N：\n'))
	if choice == 'Y':
		a = 'truncate %s' % table_insert_to
		# print(a)
		cursor.execute(a)  # 清空原来库中的专家名单

	print('输入抽取的参数\n')  # 输入抽取树，构造抽取多叉树

	class TreeNode(object):
		def __init__(self, num):
			self.num = num
	spec_extract_tree = Tree()
	batch_num = int(input('抽取几批专家，请输入数字：'))
	spec_extract_tree.create_node('Batch', 'batch', data=TreeNode(batch_num))  # root node
	sum_num = 0
	for i in range(0, batch_num):
		print('请输入当前第（%d / %d） 批抽取的专家组数：' % ((i + 1), batch_num))
		group_num = int(sys.stdin.readline())
		spec_extract_tree.create_node('Group', 'group'+str(i), parent='batch', data=TreeNode(group_num))

		for j in range(0, group_num):
			print('请输入当前第 %d 批，第（%d / %d）组抽取的专家类别：' % ((i + 1), (j+1), group_num))
			cate_name = (str(sys.stdin.readline()).strip('\n'))
			spec_extract_tree.create_node('Category', str(i)+'category'+str(j), parent='group'+str(i), data=TreeNode(cate_name))
			print('请输入当前第 %d 批，第（%d / %d）组，%s 类专家抽取的数量是：' % ((i + 1), (j+1), group_num, cate_name))
			extract_num = int(sys.stdin.readline())
			sum_num = sum_num + extract_num
			spec_extract_tree.create_node('Extract_Num', str(i)+'extract_num'+str(j), parent=str(i)+'category'+str(j), data=TreeNode(extract_num))



			# print('执行随机抽取和结果写入函数')
			# 判断专家是否已经被抽取完
			cate_sql = "select * from %s where category = '%s';" % (table_select_from, cate_name)
			out_sql = "select * from %s where category = '%s';" % (table_insert_to, cate_name)
			#print('cate_sql = ', cate_sql)
			#print('out_sql = ', out_sql)
			cate_num = cursor.execute(cate_sql)
			#print('cate_num =', cate_num)
			out_num = cursor.execute(out_sql)
			#print('out_num =', out_num)
			s = int(cate_num - out_num)
			print('专家库中还有%d位该类专家可供抽取。' % s)
			if s > 0:
				select_result = random_select(table_select_from, cate_name, extract_num)
				result_insert(select_result, table_insert_to, cate_name, table_select_from)
				print('第(%d/%d)批，第(%d/%d)组，抽取 %s 类，%d个专家完成！' % ((i + 1), batch_num, (j+1), group_num, cate_name, extract_num))
			else:
				print('可用专家不足，终止抽取')
				break
			# print('随机抽取和结果写入函数执行完毕')
	# 显示抽取的树结构
	# spec_extract_tree.show()
	print('\n随机抽取的设计如下：')
	spec_extract_tree.show(data_property='num')
	return batch_num, sum_num
	# 循环完成后专家抽取树构造完毕、抽取完毕、抽取结果写入完毕


if __name__ == '__main__':
    # 连接数据库
    # noinspection PyBroadException
    print('1、连接中...\n')
    try:
        db = pymysql.connect(host='localhost', user='root', passwd='123123123', charset='utf8', port=3306)
    except:
        print('Could not connect to mysql server')

    cursor = db.cursor()
    cursor.execute('create database if not exists SpecList;')
    cursor.execute('use SpecList;')

    print('2、获取数据\n')
    sheet = open_excel()

    print('3、载入数据\n')
    # 创建表namelist，用于存放专家信息
    cursor.execute('create table if not exists namelist (\
                    spec_id varchar(20), \
                    name varchar(20), \
                    profession varchar(50), \
                    dept2 varchar(20), \
                    dept3 varchar(20), \
                    title varchar(20), \
                    category varchar(20), \
                    hiredate varchar(20), \
                    source varchar(20), \
                    contact varchar(20));')

# ----------------------------以下为自定义的mysql存储过程--------------------------------
    # 该过程的功能：1，判断namelist中是否有索引列 2，如果没有，则在namelist表中创建索引并将其设置为主键，用于专家随机抽取时的索引，然后放到表的第一列

    # 如果之前定义过同名的存储过程，则删除
    cursor.execute('drop procedure if exists pro_a;')

    # 定义一个存储过程pro_a，功能是：输入为0时，执行索引创建、设置主键、放置到第一列的操作
    cursor.execute('create procedure pro_a(in s int)\
    begin\
    if (s=0) then\
    ALTER TABLE namelist ADD spec_index INT(4) NOT NULL PRIMARY KEY AUTO_INCREMENT FIRST;\
    end if;\
    end ')

    # 变量赋值，查看namelist中是否已存在spec_index，不存在则设置index_exist为0
    cursor.execute('SELECT COUNT(*) into @index_exist FROM information_schema.columns WHERE TABLE_NAME="namelist" AND COLUMN_NAME="spec_index"')

    # 调用之前定义的存储过程
    cursor.execute('call pro_a(@index_exist)')
# -------------------------------------------------------------------------------

    insert_data(sheet, 'namelist')
    # 创建表extract_result，用于存放抽取结果
    cursor.execute('create table if not exists extract_result (\
                    spec_index int, \
                    spec_id varchar(20), \
                    name varchar(20), \
                    profession varchar(50), \
                    dept2 varchar(20), \
                    dept3 varchar(20), \
                    title varchar(20), \
                    category varchar(20), \
                    hiredate varchar(20), \
                    source varchar(20), \
                    contact varchar(20));')

    print('目前所有专家类别：', end= '')
    cursor.execute('select distinct category from namelist where category is not NULL;')
    print(cursor.fetchall())

    batch_sum = 0
    sum_sum = 0
    while 1:
        message = input('是否继续抽取？请输入Y或N：')
        if message == 'N':
            break
        else:
            print('\n执行随机抽取\n')
            (batch_num, sum_num) = input_extract_tree('namelist', 'extract_result')
            batch_sum = batch_sum + batch_num
            sum_sum = sum_sum + sum_num

    print('4、随机抽取结果：', end='')
    print('总共抽取了%d个批次，共%d位专家，列表如下：' % (batch_sum, sum_sum))
    cursor.execute('select * from extract_result;')
    result_list = cursor.fetchall()
    for i in range(0, len(result_list)):
        print(result_list[i])

    print('\n5、随机抽取已完成并保存，连接关闭，按回车键退出\n')
    cursor.close()
    db.close()
	#  random_select()
input()
