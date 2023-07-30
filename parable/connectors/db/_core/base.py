from sqlalchemy import Connection, create_engine, Engine, URL

from .controller import DBController


class ConnectionHandler(DBController):

    engine: Engine = None
    conn: Connection = None

    def __init__(self, url: URL):
        """
        """
        self._url = url


    def __enter__(self):
        return self
    

    def __exit__(self, exc_type, exc_value, traceback):
        self.__close()


    def __connect(self):
        """
        """
        self.conn = self.engine.connect()
        print(f"{self._url.drivername} connected.")


    def __close(self):
        """
        """
        if self.conn:
            self.conn.close()
            print(f"{self._url.drivername} connection closed.")

        if self.engine:
            self.engine.dispose()
            print(f"{self._url.drivername} engine disposed.")


    @property
    def engine(self) -> Engine:
        return create_engine(url=self._url)


    def connect(self):
        self.__connect()
        return self
    

    def close(self):
        self.__close()

