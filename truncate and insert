#!/usr/bin/python3

import logging
import logging.handlers
import configparser
import psycopg2
import os
import sys

logger = None
now = datetime.datetime.now()

class Logger:
    def __init__(self, size=0, backup_cnt=0, pid=False):
		    global logger
		
		    logger = logging.getLogger("sample")
		
		    self.filename = f"./log/ResourceTable_{date:%Y%m%d}.log"
		    self.filesize = size
		    self.backup_count = backup_cnt
		    self.file_handler = None
		
		def setLogger(self):
		    if self.file_handler:
				    logger.removeHandler(self.file_handler)
						
				logger.setLevel(logging.INFO)
				formatter = logging.Formatter("[%(asctime)s %(levelname)s] %(message)s")
				
				self.file_handler = logging.handlers.RotatingFileHandler(self.fimename, mode='a', maxBytes=self.filesize, backupCount=self.backup_count)
				self.file_handler.setFormatter(formatter)
				logger.addHandler(self.file_handler)
				
				
				
class Worker:
    def __init__(self, argv):
		    if len(argv) != 2:
				    print(f"Usage: {argv[0]} config.ini")
						
				
				self.logObj = Logger()
				self.config_file = argv[1]
				self.config = configparser.ConfigParser()
				self.config.read(self.config_file)
				
				self.postgre_host = ''
				self.postgre_port = ''
				self.postgre_db = ''
				self.postgre_user = ''
				self.postgre_pass = ''
				
				self.sleep = 0
				
		def setLog(self):
		    section = "LOGGER"
				filesize = self.config.getint(section, "filesize")
				backup_count = self.config.getint(section, "backup_count")
				enable_pid_postfix = self.config.getboolean(section, "enable_pid_postfix")
				
				self.getObj = Logger(filesize, backup_count, enable_pid_postfix)
				self.getObj.setLogger()
				
		def setDatas(self):
		    section = "SERVER"
				self.postgre_host = self.config.get(section, "POSTGRE_HOST")
				self.postgre_port = self.config.get(section, "POSTGRE_PORT")
				self.postgre_db = self.config.get(section, "POSTGRE_DB")
				self.postgre_user = self.config.get(section, "POSTGRE_USER")
				self.postgre_pass = self.config.get(section, "POSTGRE_PASS")
				
		def runProcess(self):
		    conn = psycopg2.connect(host=self.postgre_host, dbname=self.postgre_db, user=self.postgre_user, password=self.postgre_pass, port=self.postgre_port)
				curs = conn.cursor()
				
				curs.execute("SELECT COUNT(*) FROM TABLE;")
				count1 = curs.fetchone()
				logger.info("PRE >> TABLE : " + str(count1[0]))
				curs.execute("SELECT COUNT(*) FROM TABLE_his;")
				count2 = curs.fetchone()
				logger.info("PRE >> TABLE_his : " + str(count2[0]))
				count1 = count1[0]
				count2 = count2[0]
				
				try:
				    if count1 >= count2:
						    curs.execute("TRUNCATE TABLE TABLE_his;")
								curs.exevute("INSERT INTO TABLE_his SELECT *, now() FROM TABLE;")
								conn.commit()
								
								logger.info(f"{now:%Y-%m-%d %H:%M}  ---- Table Copied")
								
						else:
						    logger.info(f"{now:%Y-%m-%d %H:%M}  ---- PASS")
								
				except:
				    logger.info("[ERROR] time out")
						
				
				curs.execute("SELECT count(*) FROM TABLE;")
				count1 = curs.fetchone()
				logger.info("POST >> TABLE : " + str(count1[0])
				curs.execute("SELECT count(*) FROM TABLE_his;")
				count2 = curs.fetchone()
				logger.info("POST >> TABLE_his : " + str(count2[0])
				
				curs.close()
				conn.close()
				
    def run(self):
		    self.setLog()
				self.setDatas()
				self.runProcess()
				
				
	if __name__ == '__main__':
	    processor = Worker(sys.argv)
			processor.run()
			
				
