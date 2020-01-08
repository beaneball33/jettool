import tejapi
tejapi.ApiConfig.api_key = "your_API_key"
class financial_report(object):
    def __init__(self):
        self.accountData = None
        self.activeAccountData  = None	
    def inital_report(self):
        self.accountData = tejapi.get('TWN/AIACC')
        self.activeAccountData = tejapi.get('TWN/AINVFACC_INFO_C')
        self.accountData['cname'] = self.accountData['cname'].str.replace('(','（').replace(')','）')
        self.accountData = self.accountData.sort_values(by=['cname'])
        self.accountData = self.accountData.drop_duplicates(subset=['code'],keep='last')
