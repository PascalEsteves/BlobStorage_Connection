import os
import json
from .config import Valid_Blob_Config
from .blob_connection import Connection

# f = open('config.json')
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
f = open(os.path.join(__location__, 'config.json'))
config = Valid_Blob_Config(json.load(f))

class Blob_Connection():
  def __init__(self):
    self.connection = Connection(config)

