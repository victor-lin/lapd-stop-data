import os.path

ORACLE_SERVER = 'oracle.cise.ufl.edu'
ORACLE_SID = 'orcl'
ORACLE_PORT = 1521

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(os.path.dirname(BASE_DIR), 'Stop_Data_Open_Data-2015.csv')
