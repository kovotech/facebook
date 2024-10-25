import requests
import hmac, hashlib
import json

def hash_hmac(app_secret:str,access_token:str):
    hashed_str = hmac.new(app_secret.encode('utf-8'),access_token.encode('utf-8'),digestmod=hashlib.sha256).hexdigest()
    return hashed_str

class FBApi:
    def __init__(self,host,app_id,app_secret,app_version,client_token,access_token,fb_account_id) -> None:
        self.host=host,
        self.app_version=app_version
        self.app_id=app_id
        self.app_secret=app_secret
        self.client_token=client_token
        self.access_token=access_token
        self.fb_account_id=fb_account_id

    def get_traffic_cost_data_by_date(self,fields:str,level:str,limit:int,date:str):
        url = f"{self.host[0]}/{self.app_version}/act_{self.fb_account_id}/insights"
        params = {
            "access_token":self.access_token,
            "appsecret_proof":hash_hmac(self.app_secret,self.access_token),
            "fields":fields,
            "level":level,
            "limit":limit,
            "time_range":"{"+f"since:'{date}',until:'{date}'"+"}"
        }
        response = requests.get(url=url,params=params)
        if response.status_code==200:
            data = json.loads(response.text)['data']
            return data
        else:
            return {"errorCode":response.status_code,"errorDescription":response.text}