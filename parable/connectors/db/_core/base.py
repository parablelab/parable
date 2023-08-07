from sqlalchemy import Connection, create_engine, Engine, URL
from .controller import DBController


class ConnectionHandler(DBController):
    """
    A context manager class for managing database connections and engines.

    This class provides a standard contextual interface for managing database 
    connections and engines using SQLAlchemy, ensuring proper handling including 
    connecting, disconnecting, and disposing.

    Args:
        url (URL): The database URL to connect to.

    Example:
        url = URL(drivername='sqlite', database='mydb.sqlite')
        with ConnectionHandler(url) as conn_handler:
            # Use conn_handler.conn and conn_handler.engine here
            pass
        # Connection and engine are properly closed and disposed after the block.

    """

    _engine: Engine = None
    _conn: Connection = None


    def __init__(self, url: URL):
        """
        Initialize a ConnectionHandler instance.

        Args:
            url (URL): The database URL to connect to.

        """
        self._url = url


    def __enter__(self):
        """
        Enter the context for the connection handler.

        Returns:
            ConnectionHandler: The ConnectionHandler instance.

        """
        return self
    

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the context for the connection handler.

        Args:
            exc_type: The type of exception raised, if any.
            exc_value: The exception object raised, if any.
            traceback: The traceback for the exception, if any.

        """
        self.__close()


    def __connect(self):
        """
        Open a database connection.

        """
        self._conn = self.engine.connect()
        print(f"{self._url.drivername} connected.")


    def __close(self):
        """
        Close the database connection and dispose of the engine.

        """
        if self._conn:
            self._conn.close()
            print(f"{self._url.drivername} connection closed.")

        if self._engine:
            self._engine.dispose()
            print(f"{self._url.drivername} engine disposed.")


    @property
    def conn(self):
        """
        Get the active database connection.

        Returns:
            Connection: The active database connection.

        """
        return self._conn


    @property
    def engine(self):
        """
        Get or create the database engine.

        Returns:
            Engine: The database engine.

        """
        if not self._engine:
            self._engine = create_engine(url=self._url, **self._kwargs)
        return self._engine
    

    @engine.setter
    def engine(self, engine: Engine):
        """
        Set the database engine and close the previous connection and dispose of prior engine.

        Args:
            engine (Engine): The database engine to set.

        """
        self.__close()
        self._engine = engine
    

    def connect(self, **kwargs):
        """
        Connect to the database with optional keyword arguments.

        Args:
            **kwargs: Additional keyword arguments to pass when creating the engine.

        Returns:
            ConnectionHandler: The ConnectionHandler instance.

        """
        self._kwargs = kwargs
        self.__connect()
        return self
    

    def close(self):
        """
        Close the database connection and dispose of the engine.

        """
        self.__close()

