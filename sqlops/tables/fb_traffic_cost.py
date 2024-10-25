from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, DateTime, BigInteger, DECIMAL, Integer, text, Boolean, Time, Text
import sqlalchemy
from sqlalchemy.schema import CreateTable

class Fb_Traffic_Cost:
    def __init__(self,engine:sqlalchemy.engine.Engine) -> None:
        self.engine=engine

    @staticmethod
    def getTable() -> Table:
        TABLE = Table(
            "fb_traffic_cost",MetaData(),
            Column('id',String(500),primary_key=True),
            Column('campaign_id',Text),
            Column('campaign_name',Text),
            Column('impressions',Text),
            Column('clicks',Integer),
            Column('inline_link_clicks',Integer),
            Column('spend',Text),
            Column('buying_type',Text),
            Column('date',Text)
        )
        return TABLE
    
    def create_table(self):
        table = Fb_Traffic_Cost.getTable()
        stmt = CreateTable(table,if_not_exists=True)
        with self.engine.begin() as connx:
            connx.execute(stmt)

    @staticmethod
    def map(src:dict):
        output_dict:dict = {}
        try:
            output_dict['id'] = str(src['campaign_id'])+"_"+str(src['date_start'])
        except:
            output_dict['id'] = None
        try:
            output_dict['campaign_id'] = src['campaign_id']
        except:
            output_dict['campaign_id'] = None
        try:
            output_dict['campaign_name'] = src['campaign_name']
        except:
            output_dict['campaign_name'] = None
        try:
            output_dict['impressions'] = src['impressions']
        except:
            output_dict['impressions'] = 0
        try:
            output_dict['clicks'] = src['clicks']
        except:
            output_dict['clicks'] = 0
        try:
            output_dict['inline_link_clicks'] = src['inline_link_clicks']
        except:
            output_dict['inline_link_clicks'] = 0
        try:
            output_dict['spend'] = src['spend']
        except:
            output_dict['spend'] = 0
        try:
            output_dict['buying_type'] = src['buying_type']
        except:
            output_dict['buying_type'] = None
        try:
            output_dict['date'] = src['date_start']
        except:
            output_dict['date'] = None
                
        return output_dict
    
    @staticmethod
    def get_insert_stmt(record:dict):
        payload = Fb_Traffic_Cost.map(record)
        table = Fb_Traffic_Cost.getTable()
        stmt = table.insert().values(payload)
        return stmt
        
    @staticmethod
    def get_delete_stmt(record:dict):
        payload = Fb_Traffic_Cost.map(record)
        table = Fb_Traffic_Cost.getTable()
        stmt = table.delete().where(table.c.id==payload['id'])
        return stmt
        
    def import_to_sql(self,record:dict):
        with self.engine.begin() as connx:
            deleteStmt = Fb_Traffic_Cost.get_delete_stmt(record)
            insertStmt = Fb_Traffic_Cost.get_insert_stmt(record)
            connx.execute(deleteStmt)
            connx.execute(insertStmt)