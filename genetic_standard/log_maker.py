
from pytz import timezone, utc
import pytz
tz = pytz.timezone('Asia/Shanghai')
import datetime
import logging
import os


def custom_time(*args):
    utc_dt = utc.localize(datetime.datetime.utcnow())
    my_tz = timezone("Asia/Shanghai")
    converted = utc_dt.astimezone(my_tz)
    return converted.timetuple()


logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
if not os.path.exists(r'./logs'):
    os.mkdir('./logs')
    if not os.path.exists('./tdsn_log.txt'):
        fp = open("./logs/tdsn_log.txt", 'w')
        fp.close()
handler = logging.FileHandler("./logs/tdsn_log.txt", encoding="UTF-8")
handler.setLevel(logging.INFO)
logging.Formatter.converter = custom_time
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

