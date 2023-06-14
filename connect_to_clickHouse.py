import pandahouse as ph
import pandas as pd
import clickhouse_connect


class ClickHouse_Pipe(object):
    def __init__(self, host: str, port: str, DBname: str, UserName: str, Password: str) -> None:
        """
        Connect to clickhouse and make sql operation on, such as uploading a table, reading, or just run operational sql like creating, deleting, or altering

        INPUTS:
        host: the Server IP of the database.
        port: port to connect.
        DBname: database name.
        UserName: user name (have permission).
        Password: user password.
        """

        print(f'Connecting to {DBname} ...')
        try:
            self.connection = {'host' : f"http://{host}:{port}", 'database' : DBname, 'user': UserName, 'password' : Password } 
            self.Houseclient = clickhouse_connect.get_client(database= DBname, host= host, port= port, user= UserName, password= Password)
            print('Done, Connection established.')
        except:
            print('Failed to connecting DB !!!')


    def ClickHouse_Upload(self, data: pd.DataFrame ,tableName: str) -> None:
        """
        Method to upload pandas data frame to clickhouse table 

        INPUT:
        data: pandas data frame carry the needed data to upload.
        tableName: the name of the clickhouse table to upload data to.
        """
        try:
            ph.to_clickhouse(data, tableName, index=False, chunksize=100000, connection=self.connection)
            print('Uploaded Successfuly !!')
        except Exception as e:
            print('Erorr >>> '+str(e))

    def ClickHouse_Read(self, query: str) -> None:
        """
        Methode to read clickhouse table as pandas data frame

        INPUT:
        query: the sql query needed to read the table.
        """
        df = ph.read_clickhouse(query, connection=self.connection)
        print(df)

    def Run_SQL(self, query: str) -> None:
        """"
        Methode to run operational sql for (DELETE, ALTER, CREATE, ... etc)

        INPUT:
        query: the sql query run.
        """
        response= self.Houseclient.query(query=query)
        print(response.result_rows)

    def closeSession(self) -> None:
        """
        End Session
        """
        self.Houseclient.close()