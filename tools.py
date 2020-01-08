from . import params
from .dataset import finreport
from .dataset import listedstock
import os
import tejapi
tejapi.ApiConfig.api_key = "GDEy0mWAGqnI3EemCREGREZMcEVbnF"
class financial_tool(finreport.financial_report):
    def __init__(self):
        for name in params.__dict__:
            if '__' not in name and not callable(params.__dict__.get(name)):   
                self.__dict__[name] = params.__dict__.get(name)
