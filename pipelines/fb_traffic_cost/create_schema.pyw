import os
import sys
from dotenv import load_dotenv
load_dotenv()
sys.path.append(os.environ['PROJECT_PATH_SERVER'])
from sqlops.tables.fb_traffic_cost import Fb_Traffic_Cost
from sqlops.engine import Engine

if __name__ == '__main__':
    create_table_job = Fb_Traffic_Cost(Engine.mysql(
                                        host=os.environ['SQL_HOST'],
                                        db=os.environ['SQL_DB'],
                                        user=os.environ['SQL_USER'],
                                        pswd=os.environ['SQL_PSWD']         
                                        )
                                        )
    create_table_job.create_table()