from apscheduler.schedulers.blocking import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import time
import os

scheduler = BackgroundScheduler()

def select_stock():
    os.system('./home/rying/run_select_stocks.sh')

scheduler.add_job(select_stock, CronTrigger.from_crontab('0 9 * * *'))
scheduler.start()
while True:
    time.sleep(1)