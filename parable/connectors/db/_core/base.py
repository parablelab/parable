from sqlalchemy import Connection, create_engine, Engine, URL
from .controller import DBController

class ConnectionHandler(DBController):

    _engine: Engine = None
    _conn: Connection = None


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
        self._conn = self.engine.connect()
        print(f"{self._url.drivername} connected.")


    def __close(self):
        """
        """
        if self._conn:
            self._conn.close()
            print(f"{self._url.drivername} connection closed.")

        if self._engine:
            self._engine.dispose()
            print(f"{self._url.drivername} engine disposed.")


    @property
    def conn(self):
        return self._conn


    @property
    def engine(self):
        if not self._engine:
            self._engine = create_engine(url=self._url, **self._kwargs)
        return self._engine
    

    @engine.setter
    def engine(self, engine: Engine):
        self.__close()
        self._engine = engine
    

    def connect(self, **kwargs):
        self._kwargs = kwargs
        self.__connect()
        return self
    

    def close(self):
        self.__close()

