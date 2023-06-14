import pymysql
import pandas as pd
from datetime import datetime

class GoToMysql(object):
    def __init__(self,host='localhost', port :int = None, user_name :str =None, password :str =None, db_name :str =None) -> None:
        """
        Go_to_mysql class is for connecting your MYSQL DB to python to play around.\n
        IN: 
        host: default = 'localhost'.
        user_name: The user name of the needed DB.
        password: The password to access this DB.
        db_name: The name of That DB.
        """
        print(f'Connecting to {db_name} ...')
        try:
            self.connection= pymysql.connect(
            host=host,
            port=port,
            user=user_name,
            passwd=password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor)
            print('Done, Connection established.')
        except:
            print('Failed to connecting DB !!!')
        self.corso=self.connection.cursor()

    def Execute(self,Query) -> pymysql.cursors.DictCursor:
        """
        Method to execute any mysql query on the connected DB.
        """
        self.corso.execute(Query)
        print('Query executed.')
        return self.corso

    @property
    def Show_data(self) -> None:
        """
        Property to show the all data, after exuting a query.
        """
        return self.corso.fetchall()

    def convertSqlQueryToDataFrame(self,saveAs=None,path_to_save=None) -> pd.DataFrame:
        """
        Method to show and convert the readed data as csv, Excel, or json file if needed.
        Note: you can just show up the data as pandas DataFrame without saving it into file by keep arguments as default.
        """
        my_data_frame=pd.DataFrame(self.corso.fetchall())

        if saveAs=='csv': my_data_frame.to_csv(path_or_buf=path_to_save+'\\'+self.db_name+'_'+datetime.now().strftime(r"%Y_%m_%d_%H_%M_%S")+'.csv',index=False)
        elif saveAs=='Excel': my_data_frame.to_excel(excel_writer=path_to_save+'\\'+self.db_name+'_'+datetime.now().strftime(r"%Y_%m_%d_%H_%M_%S")+'.xlsx',index=False)
        elif saveAs=='json': my_data_frame.to_json(path_or_buf=path_to_save+'\\'+self.db_name+'_'+datetime.now().strftime(r"%Y_%m_%d_%H_%M_%S")+'.json')
        else: pass
        
        return my_data_frame
    
    def closeSession(self):
        """
        End connection session
        """
        self.connection.close()
        print('Session Closed ... ')




if __name__=='__main__':
    ob=GoToMysql()
    print(ob.Execute('SELECT * FROM interface inf'))
    # ob.Show_data
    print(ob.convertSqlQueryToDataFrame())


