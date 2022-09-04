from datetime import datetime,timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
import configparser
import logging
from importlib import import_module
from sqlalchemy import create_engine, MetaData




class Base():
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    @property
    def config(self):
        self._config = configparser.ConfigParser()
        self._config.read("config.ini")
        return self._config
    
    def create_db_client(self, type, user, password, host, port, db):
        db_config = self.config["db"]
        type = db_config["type"]
        user = db_config["user"]
        password = db_config["password"]
        host = db_config["host"]
        port = db_config["port"]
        db = db_config["db"]
        url = f"{type}://{user}:{password}@{host}:{port}/{db}"
        type_lib_map = {
            "postgresql": "psycopg2"
        }
        lib = type_lib_map[type]
        import_module(lib)
        self._conn = lib.connect(url)
        self._cur = self._conn.cursor()
        return self._cur
        

class WaveAppScraper(Base):

    def __init__(self):
        self.login_url = self.config["site"]["login_url"]
        self.login_id = self.config["site"]["login_id"]
        self.password = self.config["site"]["password"]
        self.area_detail_page_url = self.config["site"]["area_detail_page_url"]
        
    def login(self, url: str, login_id: str, password: str):
        login_info = {
            "account": login_id,
            "password": password
        }
        self.session = requests.session()
        res = self.session.post(url, data=login_info)
        res.raise_for_status()

    def get_bs_from_url(self, url: str) -> BeautifulSoup:
        
        res = self.session.get(url)
        res.raise_for_status()
        bs = BeautifulSoup(res.text, "html.parser")
        required_certificate = bs.find("form")
        if required_certificate:
            self.session.post(required_certificate["action"])
            res = self.session.get(url)
            res.raise_for_status()
            bs = BeautifulSoup(res.text, "html.parser")
        return bs

    @classmethod
    def get_wave_report(cls, bs: BeautifulSoup) -> pd.DataFrame:
        
        df = pd.DataFrame(index=[], columns=["point_id", "date", "time", "score", "score_mark", "wave_size", "wind_info"])
        for div in bs.select(".point-info-wrapper"):
            point = div.select_one(".point-name").text
            date = datetime.now().date()
            try:
                div_time = div.select_one(".point-mod-time").select(".wave-gray")
                time = div_time[len(div_time)-1].text
                if div_time[0].text == "[前日]":
                    date = date - timedelta(1)
            except IndexError:
                time = div.select_one(".point-mod-time").text[-5:]
            score = div.select_one(".point-condition-score").text
            score_mark = div.select_one(".point-condition-mark").text
            wave_size = div.select_one(".point-size").text
            wind_info = div.select_one(".point-summary-div").contents[3]
            dct = {
                "point": point,
                "date": date,
                "time": time,
                "score": score,
                "score_mark": score_mark,
                "wave_size": wave_size,
                "wind_info": wind_info
            }
            df = df.append(dct, ignore_index=True)
        return df
    
    @classmethod
    def get_weather_report(cls, bs: BeautifulSoup) -> pd.DataFrame:
        df = pd.DataFrame(index=[], columns=["point_id", "date", "time", "score", "score_mark", "wave_size", "wind_info"])
    

        
    def execute(self):
        self.login(url=self.login_url, login_id=self.login_id, password=self.password)
        area_detail_page_bs = self.get_bs_from_url(url=self.area_detail_page_url)
        wave_report_df = self.get_wave_report(bs=area_detail_page_bs)
        weather_report_df = self.get_weather_report(bs=area_detail_page_bs)

    def df_to_db(self, df: pd.DataFrame, table_name: str):
        db_client = self.create_db_client()
        db_client.execute()

        
        
if __name__ ==  "__main__":
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    sc = WaveAppScraper()
    sc.execute()
