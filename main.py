import jettool.tools
if __name__ == "__main__": 
    test()
def test():
    print('creating object')
    ca = jettool.tools.financial_tool()
    print('initializiing')
    ca.inital_report()