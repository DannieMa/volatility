# coding=utf-8
import csv
#import datetime
import time
import six


def write_csv(data,filename):
	#now_time = datetime.datetime.now()
	#filename = filename +'_'+ now_time.strftime('%Y_%m_%d_%H_%M_%S')+'.csv'
	filename = filename + '.csv'
	with open(filename,"a+") as csvfile:		
		writer = csv.writer(csvfile)
		#写入多行用writerows
		writer.writerow(data)
		
def write_data_list_csv(data,filename):
	#now_time = datetime.datetime.now()
	#filename = filename +'_'+ now_time.strftime('%Y_%m_%d_%H_%M_%S')+'.csv'
	filename = filename + '.csv'
	with open(filename,"a+") as csvfile:		
		writer = csv.writer(csvfile)
		#写入多行用writerows
		for item in data:
			writer.writerow(item)

#aycn print function
def test_print_stock_daily_data(data):
	datalist = []
	datalist.append(data.contents.code)
	#a=data.contents.time
	#b = time.gmtime(a)
	#c=time.strftime("%Y-%m-%d", b)
	datalist.append(data.contents.time)
	datalist.append(data.contents.paused)
	datalist.append(data.contents.factor)
	datalist.append(data.contents.open)
	datalist.append(data.contents.close)
	datalist.append(data.contents.high)
	datalist.append(data.contents.low)
	datalist.append(data.contents.high_limit)
	datalist.append(data.contents.low_limit)
	datalist.append(data.contents.volume)
	datalist.append(data.contents.money)
	print (datalist)
	return datalist

def test_print_stock_minute_data(data):
	datalist = []
	datalist.append(data.contents.code)
	#a=data.contents.time
	#b = time.gmtime(a)
	#c=time.strftime("%Y-%m-%d %H:%M:%S", b)
	datalist.append(data.contents.time)
	datalist.append(data.contents.factor)
	datalist.append(data.contents.avg)
	datalist.append(data.contents.open)
	datalist.append(data.contents.close)
	datalist.append(data.contents.high)
	datalist.append(data.contents.low)
	datalist.append(data.contents.volume)
	datalist.append(data.contents.money)
	print (datalist)
	return datalist

def test_print_stock_tick_data(data):
	datalist = []
	datalist.append(data.contents.code)
	#a=data.contents.time
	#b = time.gmtime(a)
	#c=time.strftime("%Y-%m-%d %H:%M:%S", b)
	datalist.append(data.contents.time)
	datalist.append(data.contents.current)
	datalist.append(data.contents.status)
	datalist.append(data.contents.pre_close)
	datalist.append(data.contents.open)
	datalist.append(data.contents.high)
	datalist.append(data.contents.low)
	datalist.append(data.contents.volume)
	datalist.append(data.contents.money)
	datalist.append(data.contents.bought.a1p)
	datalist.append(data.contents.bought.a1v)
	datalist.append(data.contents.bought.a2p)
	datalist.append(data.contents.bought.a2v)
	datalist.append(data.contents.bought.a3p)
	datalist.append(data.contents.bought.a3v)
	datalist.append(data.contents.bought.a4p)
	datalist.append(data.contents.bought.a4v)
	datalist.append(data.contents.bought.a5p)
	datalist.append(data.contents.bought.a5v)
	datalist.append(data.contents.bought.a6p)
	datalist.append(data.contents.bought.a6v)
	datalist.append(data.contents.bought.a7p)
	datalist.append(data.contents.bought.a7v)
	datalist.append(data.contents.bought.a8p)
	datalist.append(data.contents.bought.a8v)
	datalist.append(data.contents.bought.a9p)
	datalist.append(data.contents.bought.a9v)
	datalist.append(data.contents.bought.a10p)
	datalist.append(data.contents.bought.a10v)
	datalist.append(data.contents.bought.b1p)
	datalist.append(data.contents.bought.b1v)
	datalist.append(data.contents.bought.b2p)
	datalist.append(data.contents.bought.b2v)
	datalist.append(data.contents.bought.b3p)
	datalist.append(data.contents.bought.b3v)
	datalist.append(data.contents.bought.b4p)
	datalist.append(data.contents.bought.b4v)
	datalist.append(data.contents.bought.b5p)
	datalist.append(data.contents.bought.b5v)
	datalist.append(data.contents.bought.b6p)
	datalist.append(data.contents.bought.b6v)
	datalist.append(data.contents.bought.b7p)
	datalist.append(data.contents.bought.b7v)
	datalist.append(data.contents.bought.b8p)
	datalist.append(data.contents.bought.b8v)
	datalist.append(data.contents.bought.b9p)
	datalist.append(data.contents.bought.b9v)
	datalist.append(data.contents.bought.b10p)
	datalist.append(data.contents.bought.b10v)
	print (datalist)
	return datalist
	
#def test_print_stock_tick_data(data):
	#datalist = []
	#datalist.append(data.contents.code)
	#datalist.append(data.contents.time)
	#datalist.append(data.contents.current)
	#datalist.append(data.contents.status)
	#datalist.append(data.contents.pre_close)
	#datalist.append(data.contents.open)
	#datalist.append(data.contents.high)
	#datalist.append(data.contents.low)
	#datalist.append(data.contents.volume)
	#datalist.append(data.contents.money)
	#datalist.append(data.contents.bought)
	#print datalist
	#return datalist	

def test_print_fund_tick_data(data):
	datalist = []
	datalist.append(data.contents.code)
	#a=data.contents.time
	#b = time.gmtime(a)
	#c=time.strftime("%Y-%m-%d %H:%M:%S", b)
	datalist.append(data.contents.time)
	datalist.append(data.contents.current)
	datalist.append(data.contents.status)
	datalist.append(data.contents.pre_close)
	datalist.append(data.contents.open)
	datalist.append(data.contents.high)
	datalist.append(data.contents.low)
	datalist.append(data.contents.iopv)
	datalist.append(data.contents.volume)
	datalist.append(data.contents.money)
	datalist.append(data.contents.bought.a1p)
	datalist.append(data.contents.bought.a1v)
	datalist.append(data.contents.bought.a2p)
	datalist.append(data.contents.bought.a2v)
	datalist.append(data.contents.bought.a3p)
	datalist.append(data.contents.bought.a3v)
	datalist.append(data.contents.bought.a4p)
	datalist.append(data.contents.bought.a4v)
	datalist.append(data.contents.bought.a5p)
	datalist.append(data.contents.bought.a5v)
	datalist.append(data.contents.bought.a6p)
	datalist.append(data.contents.bought.a6v)
	datalist.append(data.contents.bought.a7p)
	datalist.append(data.contents.bought.a7v)
	datalist.append(data.contents.bought.a8p)
	datalist.append(data.contents.bought.a8v)
	datalist.append(data.contents.bought.a9p)
	datalist.append(data.contents.bought.a9v)
	datalist.append(data.contents.bought.a10p)
	datalist.append(data.contents.bought.a10v)
	datalist.append(data.contents.bought.b1p)
	datalist.append(data.contents.bought.b1v)
	datalist.append(data.contents.bought.b2p)
	datalist.append(data.contents.bought.b2v)
	datalist.append(data.contents.bought.b3p)
	datalist.append(data.contents.bought.b3v)
	datalist.append(data.contents.bought.b4p)
	datalist.append(data.contents.bought.b4v)
	datalist.append(data.contents.bought.b5p)
	datalist.append(data.contents.bought.b5v)
	datalist.append(data.contents.bought.b6p)
	datalist.append(data.contents.bought.b6v)
	datalist.append(data.contents.bought.b7p)
	datalist.append(data.contents.bought.b7v)
	datalist.append(data.contents.bought.b8p)
	datalist.append(data.contents.bought.b8v)
	datalist.append(data.contents.bought.b9p)
	datalist.append(data.contents.bought.b9v)
	datalist.append(data.contents.bought.b10p)
	datalist.append(data.contents.bought.b10v)
	print (datalist)
	return datalist
	
def test_print_index_tick_data(data):
	datalist = []
	datalist.append(data.contents.code)
	a=data.contents.time
	b = time.gmtime(a)
	c=time.strftime("%Y-%m-%d %H:%M:%S", b)
	datalist.append(c)
	datalist.append(data.contents.current)
	datalist.append(data.contents.pre_close)
	datalist.append(data.contents.open)
	datalist.append(data.contents.high)
	datalist.append(data.contents.low)
	datalist.append(data.contents.volume)
	datalist.append(data.contents.money)
	print (datalist)
	return datalist
	
def test_print_trades_data(data):
	datalist = []
	datalist.append(data.contents.code)
	#a=data.contents.time
	#b = time.gmtime(a)
	#c=time.strftime("%Y-%m-%d %H:%M:%S", b)
	datalist.append(data.contents.time)
	datalist.append(data.contents.index)
	datalist.append(data.contents.bsc)
	datalist.append(data.contents.price)
	datalist.append(data.contents.volume)
	datalist.append(data.contents.ask)
	datalist.append(data.contents.bid)
	print (datalist)
	return datalist	
	
def test_print_orders_data(data):
	datalist = []
	datalist.append(data.contents.code)
	#a=data.contents.time
	#b = time.gmtime(a)
	#c=time.strftime("%Y-%m-%d %H:%M:%S", b)
	datalist.append(data.contents.time)
	datalist.append(data.contents.side)
	datalist.append(data.contents.price)
	datalist.append(data.contents.volume)
	datalist.append(data.contents.count)
	for item in data.contents.orders:
		datalist.append(item)
	print (datalist)
	return datalist

def test_print_a_orders_data(data):
	datalist = []
	datalist.append(data.contents.code)
	datalist.append(data.contents.time)
	datalist.append(data.contents.index)
	datalist.append(data.contents.order)
	datalist.append(data.contents.orderkind)
	datalist.append(data.contents.functioncode)
	datalist.append(data.contents.price)
	datalist.append(data.contents.volume)
	print (datalist)
	return datalist
	
#sycn	
def test_print_get_day_price(data):
	datalist = []
	for item in data:
		dateItemList = []
		dateItemList.append(item.code)
		#a=item.time
		#b = time.gmtime(a)
		#c=time.strftime("%Y-%m-%d", b)
		dateItemList.append(item.time)
		dateItemList.append(item.paused)
		dateItemList.append(item.factor)
		dateItemList.append(item.avg)
		dateItemList.append(item.open)
		dateItemList.append(item.close)
		dateItemList.append(item.high)
		dateItemList.append(item.low)
		dateItemList.append(item.high_limit)
		dateItemList.append(item.low_limit)
		dateItemList.append(item.volume)
		dateItemList.append(item.money)
		#print dateItemList
		datalist.append(dateItemList)
	return datalist
	
def test_print_s_get_minute_price(data):
	datalist = []
	for item in data:
		dateItemList = []
		dateItemList.append(item.code)
		#a=item.time
		#b = time.gmtime(a)
		#c=time.strftime("%Y-%m-%d %H:%M:%S", b)
		dateItemList.append(item.time)
		dateItemList.append(item.factor)
		dateItemList.append(item.avg)
		dateItemList.append(item.open)
		dateItemList.append(item.close)
		dateItemList.append(item.high)
		dateItemList.append(item.low)
		dateItemList.append(item.volume)
		dateItemList.append(item.money)
		#print dateItemList
		datalist.append(dateItemList)
	return datalist

def test_print_s_get_stock_tick(data):
	datalist = []
	for item in data:
		dateItemList = []
		dateItemList.append(item.code)
		#a=item.time
		#b = time.gmtime(a)
		#c=time.strftime("%Y-%m-%d %H:%M:%S", b)
		dateItemList.append(item.time)
		dateItemList.append(item.current)
		dateItemList.append(item.high)
		dateItemList.append(item.low)
		dateItemList.append(item.volume)
		dateItemList.append(item.money)
		dateItemList.append(item.bought.a1p)
		dateItemList.append(item.bought.a1v)
		dateItemList.append(item.bought.a2p)
		dateItemList.append(item.bought.a2v)
		dateItemList.append(item.bought.a3p)
		dateItemList.append(item.bought.a3v)
		dateItemList.append(item.bought.a4p)
		dateItemList.append(item.bought.a4v)
		dateItemList.append(item.bought.a5p)
		dateItemList.append(item.bought.a5v)
		dateItemList.append(item.bought.a6p)
		dateItemList.append(item.bought.a6v)
		dateItemList.append(item.bought.a7p)
		dateItemList.append(item.bought.a7v)
		dateItemList.append(item.bought.a8p)
		dateItemList.append(item.bought.a8v)
		dateItemList.append(item.bought.a9p)
		dateItemList.append(item.bought.a9v)
		dateItemList.append(item.bought.a10p)
		dateItemList.append(item.bought.a10v)
		dateItemList.append(item.bought.b1p)
		dateItemList.append(item.bought.b1v)
		dateItemList.append(item.bought.b2p)
		dateItemList.append(item.bought.b2v)
		dateItemList.append(item.bought.b3p)
		dateItemList.append(item.bought.b3v)
		dateItemList.append(item.bought.b4p)
		dateItemList.append(item.bought.b4v)
		dateItemList.append(item.bought.b5p)
		dateItemList.append(item.bought.b5v)
		dateItemList.append(item.bought.b6p)
		dateItemList.append(item.bought.b6v)
		dateItemList.append(item.bought.b7p)
		dateItemList.append(item.bought.b7v)
		dateItemList.append(item.bought.b8p)
		dateItemList.append(item.bought.b8v)
		dateItemList.append(item.bought.b9p)
		dateItemList.append(item.bought.b9v)
		dateItemList.append(item.bought.b10p)
		dateItemList.append(item.bought.b10v)
		#print dateItemList
		datalist.append(dateItemList)
	return datalist
	
def test_print_s_get_fund_tick(data):
	datalist = []
	for item in data:
		dateItemList = []
		dateItemList.append(item.code)
		#a=item.time
		#b = time.gmtime(a)
		#c=time.strftime("%Y-%m-%d %H:%M:%S", b)
		dateItemList.append(item.time)
		dateItemList.append(item.current)
		dateItemList.append(item.high)
		dateItemList.append(item.low)
		dateItemList.append(item.iopv)
		dateItemList.append(item.volume)
		dateItemList.append(item.money)
		dateItemList.append(item.bought.a1p)
		dateItemList.append(item.bought.b1p)
		dateItemList.append(item.bought.a2p)
		dateItemList.append(item.bought.a2v)
		dateItemList.append(item.bought.a3p)
		dateItemList.append(item.bought.a3v)
		dateItemList.append(item.bought.a4p)
		dateItemList.append(item.bought.a4v)
		dateItemList.append(item.bought.a5p)
		dateItemList.append(item.bought.a5v)
		dateItemList.append(item.bought.a6p)
		dateItemList.append(item.bought.a6v)
		dateItemList.append(item.bought.a7p)
		dateItemList.append(item.bought.a7v)
		dateItemList.append(item.bought.a8p)
		dateItemList.append(item.bought.a8v)
		dateItemList.append(item.bought.a9p)
		dateItemList.append(item.bought.a9v)
		dateItemList.append(item.bought.a10p)
		dateItemList.append(item.bought.a10v)
		dateItemList.append(item.bought.b1p)
		dateItemList.append(item.bought.b1v)
		dateItemList.append(item.bought.b2p)
		dateItemList.append(item.bought.b2v)
		dateItemList.append(item.bought.b3p)
		dateItemList.append(item.bought.b3v)
		dateItemList.append(item.bought.b4p)
		dateItemList.append(item.bought.b4v)
		dateItemList.append(item.bought.b5p)
		dateItemList.append(item.bought.b5v)
		dateItemList.append(item.bought.b6p)
		dateItemList.append(item.bought.b6v)
		dateItemList.append(item.bought.b7p)
		dateItemList.append(item.bought.b7v)
		dateItemList.append(item.bought.b8p)
		dateItemList.append(item.bought.b8v)
		dateItemList.append(item.bought.b9p)
		dateItemList.append(item.bought.b9v)
		dateItemList.append(item.bought.b10p)
		dateItemList.append(item.bought.b10v)
		print (dateItemList)
		datalist.append(dateItemList)
	return datalist
	
	
def test_print_s_get_trades(data):
	datalist = []
	for item in data:
		dateItemList = []
		dateItemList.append(item.code)
		#a=item.time
		# b = time.gmtime(a)
		#c=time.strftime("%Y-%m-%d %H:%M:%S", b)
		dateItemList.append(item.time)
		dateItemList.append(item.index)
		dateItemList.append(item.bsc)
		dateItemList.append(item.price)
		dateItemList.append(item.volume)
		dateItemList.append(item.ask)
		dateItemList.append(item.bid)
		print (dateItemList)
		datalist.append(dateItemList)
	return datalist


def test_print_s_get_orders(data):
	datalist = []
	for item in data:
		dateItemList = []
		dateItemList.append(item.code)
		#a=item.time
		#b = time.gmtime(a)
		#c=time.strftime("%Y-%m-%d %H:%M:%S", b)
		dateItemList.append(item.time)
		dateItemList.append(item.side)
		dateItemList.append(item.price)
		dateItemList.append(item.volume)
		dateItemList.append(item.count)
		#print dateItemList
		for index in range(len(item.orders)):
			#print item.orders[index]
			dateItemList.append(item.orders[index])
		print (dateItemList)
		datalist.append(dateItemList)
	return datalist


def test_print_s_get_orders_detail(data):
	datalist = []
	for item in data:
		dateItemList = []
		dateItemList.append(item.code)
		dateItemList.append(item.time)
		dateItemList.append(item.index)
		dateItemList.append(item.order)
		dateItemList.append(item.orderkind)
		dateItemList.append(item.functioncode)
		dateItemList.append(item.price)
		dateItemList.append(item.volume)
		print (dateItemList)
		datalist.append(dateItemList)
	return datalist


def test_print_s_get_all_securities(data):
	datalist = []
	for item in data:
		dateItemList = []
		dateItemList.append(item)
		datalist.append(dateItemList)
		print (dateItemList)
	return datalist
	
def test_print_s_get_trade_days(data):
	datalist = []
	for item in data:
		dateItemList = []
		dateItemList.append(item)
		datalist.append(dateItemList)
		print (dateItemList)
	return datalist

def test_print_s_get_count_trade_days(data):
	datalist = []
	for item in data:
		dateItemList = []
		dateItemList.append(item)
		print (dateItemList)
		datalist.append(dateItemList)
	return datalist
	
#sub

def test_print_sub_stock_tick_data(data):
	datalist = []
	datalist.append(data.contents.code)
	datalist.append(data.contents.time)
	datalist.append(data.contents.current)
	datalist.append(data.contents.high)
	datalist.append(data.contents.low)
	datalist.append(data.contents.volume)
	datalist.append(data.contents.money)
	datalist.append(data.contents.bought.a1p)
	datalist.append(data.contents.bought.b1p)
	print (datalist)
	return datalist	
	
def test_print_sub_fund_tick_data(data):
	datalist = []
	datalist.append(data.contents.code)
	datalist.append(data.contents.time)
	datalist.append(data.contents.current)
	datalist.append(data.contents.high)
	datalist.append(data.contents.low)
	datalist.append(data.contents.volume)
	datalist.append(data.contents.money)
	datalist.append(data.contents.bought.a1p)
	#print datalist
	return datalist	

def test_print_sub_index_tick_data(data):
	datalist = []
	datalist.append(data.contents.code)
	datalist.append(data.contents.time)
	datalist.append(data.contents.current)
	datalist.append(data.contents.high)
	datalist.append(data.contents.low)
	datalist.append(data.contents.volume)
	datalist.append(data.contents.money)
	#print datalist
	return datalist	
	
def test_print_sub_trade_info(data):
	datalist = []
	datalist.append(data.contents.code)
	datalist.append(data.contents.time)
	datalist.append(data.contents.index)
	datalist.append(data.contents.bsc)
	datalist.append(data.contents.price)
	datalist.append(data.contents.volume)
	datalist.append(data.contents.ask)
	datalist.append(data.contents.bid)
	#print datalist
	return datalist

def test_print_sub_Orders_Info(data):
	datalist = []
	datalist.append(data.contents.code)
	datalist.append(data.contents.time)
	datalist.append(data.contents.side)
	datalist.append(data.contents.price)
	datalist.append(data.contents.volume)
	datalist.append(data.contents.count)
	for item in data.contents.orders:
		datalist.append(item)
		#print datalist
	return datalist	
	
	
#play
def test_print_play_minute_price(data):
	#f_time = time.localtime(data.contents.time)
	#f_time = time.gmtime(data.contents.time)
	#f_t=time.strftime("%Y-%m-%d %H:%M:%S", f_time)
	#print " "
	print (data.contents.code,data.contents.time,data.contents.open,data.contents.close,data.contents.high,data.contents.low,data.contents.volume,data.contents.money)

def test_print_play_stock_tick(data):
	# b = time.gmtime(data.contents.time)
	# a=time.strftime("%Y-%m-%d %H:%M:%S", b)
	# print data.contents.code,a,data.contents.current,data.contents.high,data.contents.low,data.contents.volume,data.contents.money,data.contents.bought.a1p,data.contents.bought.b1p,data.contents.n_aap,data.contents.n_bap
	test_print_stock_tick_data(data)

def test_print_play_index_tick(data):
	#b = time.localtime(data.contents.time)
	#a=time.strftime("%Y-%m-%d %H:%M:%S", b)
	print (data.contents.code,data.contents.time,data.contents.current,data.contents.high,data.contents.low,data.contents.volume,data.contents.money)
		
def test_print_play_day_price(data):
	#data_list = test_print_get_day_price(data)
	#print data_list
	#write_csv(data_list,"play_day_price")
	#b = time.localtime(data.contents.time)
	#a=time.strftime("%Y-%m-%d", b)
	print (data.contents.code,data.contents.time,data.contents.paused,data.contents.factor,data.contents.avg,data.contents.open,data.contents.close,data.contents.high,data.contents.low)
		
def test_print_play_fund_tick(data):
	#b = time.localtime(data.contents.time)
	#b = time.gmtime(data.contents.time)
	#a=time.strftime("%Y-%m-%d %H:%M:%S", b)
	#print data.contents.code,a,data.contents.current,data.contents.high,data.contents.low,data.contents.volume,data.contents.bought.a1p,data.contents.bought.b1p,data.contents.n_aap,data.contents.n_bap
	test_print_fund_tick_data(data)


def string_to_byte_base_2_or_3(data):
	if six.PY3 and isinstance(data,str):
		return bytes(data,encoding = "utf-8")
	return data


