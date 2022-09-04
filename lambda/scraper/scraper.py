from datetime import datetime,timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
import configparser
import logging
from importlib import import_module




class Base():
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    @property
    def config(self):
        self._config = configparser.ConfigParser()
        self._config.read("config.ini")
        return self._config
    
    def create_db_client(self):
        if not hasattr(self, '_cur'):            
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
            lib = import_module(lib)
            self._conn = lib.connect(url)
            self._cur = self._conn.cursor()
        return self._cur
        

class WaveAppScraper(Base):

    def __init__(self):
        super().__init__()
        self.login_url = self.config["scrape"]["login_url"]
        self.login_id = self.config["scrape"]["login_id"]
        self.password = self.config["scrape"]["password"]
        self.area_detail_page_url = self.config["scrape"]["area_detail_page_url"]
        
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

    def get_wave_report(self, bs: BeautifulSoup) -> pd.DataFrame:
        
        df = pd.DataFrame(index=[], columns=[
            "point_id", 
            "date", 
            "time", 
            "score", 
            "score_mark", 
            "wave_size", 
            "wind_info"])
        
        for div in bs.select(".point-info-wrapper"):
            point_name = div.select_one(".point-name").text
            point_id = self.get_point_id(point_name)
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
                "point_id": point_id,
                "date": str(date),
                "time": time,
                "score": score,
                "score_mark": score_mark,
                "wave_size": wave_size,
                "wind_info": wind_info
            }
            df = df.append(dct, ignore_index=True)
        self.logger.info(df)
        return df

    def get_weather_report(self, bs: BeautifulSoup) -> pd.DataFrame:
        df = pd.DataFrame(index=[], columns=[
            "point_id", 
            "date", 
            "time", 
            "weather", 
            "temperature", 
            "precipitation", 
            "wind_direction", 
            "wind_speed", 
            "wave_hight", 
            "wave_direction", 
            "wave_period"])

        for li in bs.select(".point-style"):
            point_wave_page_url = li.a["href"]
            point_weather_page_url = point_wave_page_url.split("?")[0] + "/weathers/surf?page=fcst_wave#weather"
            weather_bs = self.get_bs_from_url(point_weather_page_url)
            point_name = weather_bs.h3.text
            point_id = self.get_point_id(point_name)
            if point_id is None:
                continue
            for tr in weather_bs.select("tr"):
                td_tags = tr.select("td")
                dct = {
                        "point_id": point_id,
                        "date": None, 
                        "time": None, 
                        "weather": None,
                        "temperature": None, 
                        "precipitation": None, 
                        "wind_direction": None,  
                        "wind_speed": None,  
                        "wave_hight": None,
                        "wave_direction": None,
                        "wave_period": None
                }
                if len(td_tags) == 7:
                    exist_param_list = [
                        "date", 
                        "time", 
                        "weather",
                        "temperature", 
                        "precipitation", 
                        "wind_direction_speed",  
                        "wave_hight_direction_period"
                        ]
                elif len(td_tags) == 6:
                    exist_param_list = [
                        "time", 
                        "weather",
                        "temperature", 
                        "precipitation", 
                        "wind_direction_speed",  
                        "wave_hight_direction_period"
                        ]

                elif len(td_tags) == 5:
                    exist_param_list = [
                        "time", 
                        "weather",
                        "temperature", 
                        "precipitation", 
                        "wind_direction_speed",  
                        ]
                else:
                    continue
                
                for param, td in zip(exist_param_list, td_tags):
                    if param == "date":
                        now_date = datetime.now().date()
                        year = str(now_date.year)
                        month = str(now_date.month)
                        day = str(td.text.split("(")[0])
                        actual_date = datetime.strptime(f"{year}-{month}-{day}","%Y-%m-%d").date()
                        if (actual_date - now_date).days >= 4:
                            dct["date"] = str(actual_date - timedelta(month=1))
                        else:
                            dct["date"] = str(actual_date)
                    elif param == "time":
                        dct["time"] = td.text + ":00"
                    elif param == "wind_direction_speed":
                        dct["wind_direction"] = td.text.split("/")[0]
                        dct["wind_speed"] = td.text.split("/")[1].replace("m", "")
                    elif param == "wave_hight_direction_period":
                        dct["wave_hight"] = td.find("div", class_="text-left").text.replace("m", "")
                        dct["wave_direction"] = td.find("div", class_="direction-kanji").text
                        dct["wave_period"] = td.find("div", class_="text-right").text.replace("秒", "")
                    else:
                        dct[param] = td.text 
                df = df.append(dct, ignore_index=True)
        # Nanは前行の値で埋める
        df = df.fillna(method="ffill")
        return df             

    def get_point_id(self, point_name):
        db_client = self.create_db_client()
        point_master_table = self.config["scrape"]["POINT_MASTER"]
        db_client.execute(f"SELECT POINT_ID FROM {point_master_table} WHERE point_name = '{point_name}';")
        try:    
            point_id = str(db_client.fetchone()[0])
        except:
            point_id = None
        return point_id

    def df_to_db(self, df: pd.DataFrame, table_name: str, insert_type="insert"):
        def create_insert_queries(df, table_name, insert_type) -> list:
            columns_list = [column for column in df]
            columns_str = ",".join(columns_list)
            insert_stmt = f"INSERT INTO {table_name} ({columns_str}) "
            queries = []
            for _, row in df.iterrows():
                values_list = [row[column] for column in columns_list]
                values_str = "'" + "','".join(values_list) + "'"
                values_stmt = f"VALUES ({values_str}) "
                end_stmt = ";"
                if insert_type == "upsert":
                    pk_str = self.config["table_pk"][table_name]
                    pk_list = pk_str.split(",")
                    on_conflict_stmt = f"on conflict ({pk_str}) "
                    do_update_stmt = "do update "
                    set_stmt_elements = []
                    for column in columns_list:
                        if not column in pk_list:
                            value = row[column]
                            set_stmt_element = f"{column} = '{value}'"
                            set_stmt_elements.append(set_stmt_element)
                    set_stmt = "set " + ", ".join(set_stmt_elements)
                    query = insert_stmt + values_stmt + on_conflict_stmt + do_update_stmt + set_stmt + end_stmt
                elif insert_type == "insert":
                    query = insert_stmt + values_stmt + end_stmt
                else:
                    raise ValueError("insert_typeはinsertかupsertを指定してください")

                queries.append(query)

            return queries

        def exec_queries(queries: list) -> None:
            for query in queries:
                self.logger.info(f"query: {query}")
                db_client.execute(query)
                self.logger.info("正常終了")
            self.logger.info("全てのクエリが正常終了しました")
            db_client.execute("COMMIT;")

        db_client = self.create_db_client()
        queries = create_insert_queries(df, table_name, insert_type)
        exec_queries(queries)

    def execute(self):
        self.login(url=self.login_url, login_id=self.login_id, password=self.password)
        area_detail_page_bs = self.get_bs_from_url(url=self.area_detail_page_url)
        self.logger.info("wave_reportの作成を開始します")
        wave_report_df = self.get_wave_report(bs=area_detail_page_bs)
        self.df_to_db(df=wave_report_df, table_name="WAVE_REPORT", insert_type="upsert")
        self.logger.info("wave_reportの作成が完了しました")
        self.logger.info("weather_reportの作成を開始します")
        weather_report_df = self.get_weather_report(bs=area_detail_page_bs)
        self.df_to_db(df=weather_report_df, table_name="WEATHER_REPORT", insert_type="upsert")
        self.logger.info("weather_reportの作成が完了しました")



if __name__ ==  "__main__":
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    sc = WaveAppScraper()
    sc.execute()
