import pymssql
import pandas as pd

class Connect_SQL_Server(object):
    def __init__(self, host, port, DBname, username, password) -> None:
        """
        Connect to MS SQL server data base 

        INPUT:
        host: the Server IP of the database.
        port: port to connect.
        DBname: database name.
        UserName: user name (have permission).
        Password: user password.
        """
        print(f'Connecting to {DBname} ...')
        try:
            self.conn = pymssql.connect(server=host, user=username, port= port, password=password, database=DBname)
            self.cursor = self.conn.cursor()  
            print('Done, Connection established.')
        except:
            print('Failed to connecting DB !!!')


    def executeSql(self, query: str) -> None:
        """
        Methode to run operational sql 

        INPUT:
        query: the sql query run.
        """
        self.cursor.execute(query)
        print('Query excuted.')
    
    def readTable(self, query: str) -> pd.DataFrame:
        """
        Methode to extract tables with sql query
        """
        self.cursor.execute(query)
        return pd.DataFrame(self.cursor.fetchall(), columns= [item[0] for item in self.cursor.description])
    
    def SubmittChanges(self) -> None:
        """
        To submitt changes happened to the real database
        """
        self.conn.commit()

    def closeSession(self) -> None:
        """
        End Session
        """
        self.conn.close()
        print('session closed')


if __name__=='__main__':
    server = '192.168.0.0'
    port='1433'
    database = 'name'
    username = 'user'
    password = '123'

    conn = Connect_SQL_Server(host=server, username=username, port= port, password=password, DBname=database)
    row = conn.readTable('SELECT Top 2 * FROM Tickets t;')
    print(row)
    # pass