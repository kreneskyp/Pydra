try:
    import pysqlite2.dbapi2
except ImportError:
    pysqlite2 = None
    print "Warning: pysqlite2 couldn't be found, SQLiteBackend will not work."

from pydra.util.key import keyable

@keyable
class SQLiteBackend(object):
    """
    Backend for interfacing with SQLite databases.
    """

    def __init__(self):
        if not pysqlite2:
            raise Exception, "SQLite support could not be found."

        self.handle = None

    def __del__(self):
        self.disconnect()

    def connect(self, db):
        """
        Open a database.

        Relative paths may not be handled correctly.

        The special name ":memory:" will open a temporary memory-backed
        database.
        """

        if not self.handle:
            self.handle = pysqlite2.dbapi2.Connection(db)

    def disconnect(self):
        if self.handle:
            self.handle.close()

    @property
    def connected(self):
        return bool(self.handle)
