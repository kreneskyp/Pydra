database_names = {
    "sqlite2": "pysqlite2.dbapi2",
    "sqlite3": "sqlite3",
    "mysql": "MySQLdb",
    "postgres": "psycopg2",
}

databases = {}

for name in database_names:
    try:
        databases[name] = __import__(database_names[name], fromlist=["bogus"])
    except ImportError:
        print "Warning: Couldn't import %s!" % database_names[name]
        print "Warning: Disabling support for %s databases." % name

# Quirk database names.
if "sqlite3" in databases:
    databases["sqlite"] = databases["sqlite3"]
elif "sqlite2" in databases:
    databases["sqlite"] = databases["sqlite2"]

from pydra.util.key import keyable

@keyable
class SQLBackend(object):
    """
    Backend for interfacing with SQL databases.
    """

    handle = None

    def __init__(self, db, *args, **kwargs):
        if db in databases:
            self.dbapi = databases[db]
        else:
            raise ValueError, "Database %s not supported" % db

    def __del__(self):
        self.disconnect()

    def connect(self, *args, **kwargs):
        """
        Open a database.

        Relative paths may not be handled correctly.
        """

        if not self.handle:
            self.handle = self.dbapi.connect(*args, **kwargs)

    def disconnect(self):
        if self.handle:
            self.handle.close()
        self.handle = None

    @property
    def connected(self):
        return bool(self.handle)
