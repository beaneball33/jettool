from . import querybase
class financial_report(querybase.query_base):
    def __init__(self):
        self.accountData = None
        self.activeAccountData  = None	
    def inital_report(self):
        self.accountData = self.tejapi.get('TWN/AIACC')
        self.activeAccountData = self.tejapi.get('TWN/AINVFACC_INFO_C')
        self.accountData['cname'] = self.accountData['cname'].str.replace('(','（').replace(')','）')
        self.accountData = self.accountData.sort_values(by=['cname'])
        self.accountData = self.accountData.drop_duplicates(subset=['code'],keep='last')
