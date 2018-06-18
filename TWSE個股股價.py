# -*- coding: UTF-8 -*-

import pymysql,time,datetime,re,sys,string,requests,math
from bs4 import BeautifulSoup
import datetime
from datetime import timedelta,date
from datetime import datetime

db = pymysql.connect(host="127.0.0.1", user="root", passwd="s8661291234", db="stock_ti", charset='utf8')
cursor = db.cursor()

#抓取股票代號列表
#stock_list_sql = "select DISTINCT(stockid) from `twse_stock` order by stockid asc"
#cursor.execute(stock_list_sql)
stock_list =['1101','1102']

#for a in cursor.fetchall():
	#stock_list.append(a)

def work(stock_list):
	print(str(datetime.now()) + "開始抓取")
	today = str(datetime.now()).split(' ')[0].replace('-','')
	today = int(today)

	"""
	#從資料庫中找最後抓取日當作開始年月
	sql = "select MAX(date) from `twse_stock` where sn = (select max(sn) from `twse_stock`)"
	cursor.execute(sql)
	LA_date = cursor.fetchall()[0][0]
	"""
	LA_date = None	#避免資料庫的修改，假設沒抓到最後年月日，就以2011/01開始
	
	if LA_date != None:
		last_date = str(LA_date)
		last_date = last_date.split("-")
		start_year = last_date[0]
		start_month = last_date[1]
	else:
		start_year = '2011'
		start_month = '01'
	
	#以今日當作結束年月
	end_year = int(str(datetime.now()).split(' ')[0].split('-')[0])
	end_month = int(str(datetime.now()).split(' ')[0].split('-')[1])

	if int(start_month) < 10:
		start_month = '0'+str(int(start_month))

	if int(end_month) < 10:
		end_month = '0'+str(int(end_month))
		
	#製作應抓取年月清單
	period_list = []
	temp_y = int(start_year)
	temp_m = start_month
	while int(str(temp_y)+str(temp_m))<=int(str(end_year)+str(end_month)):#int(str(end_year)+str(end_month)):
		period_list.append([temp_y,temp_m])
		temp_m = int(temp_m)+1
		
		if int(temp_m) < 10:
			temp_m = '0' + str(int(temp_m))
		if int(temp_m)>12:
			temp_y+= 1
			temp_m = '01'

	#開始依股票清單與年月清單跑迴圈抓取
	for period in period_list:
		for k in stock_list:
			print(k,period,datetime.now(),sep = " ")
			time.sleep(3)
			stock = k
			#print str(stock)+'-'+str(y)+'
			y = period[0]
			month = period[1]
			stockdate = int(str(y)+str(month)+'01')
			if stockdate > today:
				break
			try_times = 0
			website = 'http://www.tse.com.tw/exchangeReport/STOCK_DAY?response=json&date='+str(stockdate)+'&stockNo='+str(stock)
			while try_times<5:
				try:
					url_get = requests.get(website)
					try_times = 100
				except:
					time.sleep(5)
					try_times+=1
			
			all_date = str(BeautifulSoup(url_get.text,"html.parser"))
			all_date = all_date[all_date.find('"data":[[')+len('"data":[['):all_date.find(']],"notes"')]
			all_date = str(all_date).split('],[')
			save_list = all_date

			for a in all_date:
				save_list = a.split('","')#.replace('"','')
				try:
					date = save_list[0].replace('"','').split('/')
					date = str(int(date[0])+1911)+'-'+str(date[1])+'-'+str(date[2])
				except BaseException as e:
					pass

				try:
					shares_traded_num = save_list[1].replace(',','').replace('X','')
					shares_traded_price = save_list[2].replace(',','').replace('X','')
					opening_price = save_list[3].replace(',','').replace('X','')
					highest_price = save_list[4].replace(',','').replace('X','')
					lowest_price = save_list[5].replace(',','').replace('X','')
					closing_price = save_list[6].replace(',','').replace('X','')
					Price_difference = save_list[7].replace(',','').replace('X','')
					transactions_num = save_list[8].replace(',','').replace('"','').replace('X','')
					#insert_sql = "INSERT INTO `twse_stock` (`sn`, `stockid`, `date`, `shares_traded_num`, `shares_traded_price`, `opening_price`, `highest_price`, `lowest_price`, `closing_price`, `Price_difference`,`transactions_num`) VALUES (NULL, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');"%\
					#(stock,date,shares_traded_num,shares_traded_price,opening_price,highest_price,lowest_price,closing_price,Price_difference,transactions_num)
					#cursor.execute(insert_sql)
					#db.commit()
					print(stock,date,shares_traded_num,shares_traded_price,opening_price,highest_price,lowest_price,closing_price,Price_difference,transactions_num)
				except BaseException as e:
					if str(e).find('key')!=-1 or str(e).find('--')!= -1 or str(e).find('opening')!= -1 :
						pass
					else:
						print(e)
						print(stock,date)#,shares_traded_num,shares_traded_price,save_list[3],save_list[4],save_list[5],save_list[6],save_list[7],transactions_num
					#pass
	print(str(datetime.now()) + "結束抓取")

if __name__ == '__main__':
	work(stock_list)
