import re, os,sys
from subprocess import PIPE,Popen
import psycopg2
import cStringIO
from pprint import pprint
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
e=sys.exit

# dict g - is a calling env Globals()
opt= g['opt']
MYSQL_CLIENT_HOME = g['MYSQL_CLIENT_HOME']


	
def extract(env):
	
	in_qry=open(opt.mysql_query_file, "r").read().strip().strip(';')
	db_client_dbshell=r'%s\mysql.exe' % MYSQL_CLIENT_HOME.strip('"')
	#print db_client_dbshell
	#e(0) #-u alex -pmysql_pwd -D test -h localhost
	loadConf=[ db_client_dbshell ,'-u', opt.mysql_user,'-p%s' % opt.mysql_pwd,'-D',opt.mysql_db_name, '-h', opt.mysql_db_server]
	
	header_str=''
	#if opt.ora_add_header:
	#	header_str=' CSV HEADER'
	limit=''
	if opt.mysql_lame_duck>0:
		limit='LIMIT %d' % opt.mysql_lame_duck
	quote=''
	if opt.mysql_lame_duck>0:
		quote='QUOTE  \'%s\'' % opt.mysql_quote
		
	#select * from crime_test
	#OUTFILE 'c:/tmp/orders.csv'
	out_file='c:/tmp/orders.csv'
	if os.path.isfile(out_file):
		os.remove(out_file)
	
	
	q="""
%s %s
INTO OUTFILE '%s'
FIELDS TERMINATED BY '%s'
ENCLOSED BY '%s'
LINES TERMINATED BY '\r\n';
	""" % (in_qry, limit, out_file, opt.mysql_col_delim,opt.mysql_quote)
	#print q
	#e(0)
	p1 = Popen(['echo', q], stdout=PIPE,stderr=PIPE,env=env)
	
	#pprint(loadConf)
	#print ' '.join(loadConf)
	p2 = Popen(loadConf, stdin=p1.stdout, stdout=PIPE,stderr=PIPE)
	output=' '
	status=0
	if 1:
		while output:
			output = p2.stdout.readline()
			print output
	if 1:
		err=' '
		while err:
			err = p2.stderr.readline()
			print err
			
	#e(0)
	p1.wait()
	p2.wait()
	assert os.path.isfile(out_file), 'Spool file does not exists.'
	fo = open(out_file, 'r')
	return fo

	


	