loggerhead
==========

A minimal CT log server focused on optimizing out merge delay time.  The server
will only return an SCT if a new tree head covering the certificate has already
been logged in the database.

The server relies on a database with the following tables (using postgres
data types):

```
CREATE TABLE certificates (timestamp BIGINT, tree_size BIGINT, frontier BYTEA, cert BYTEA);
```

Edit `db_config.go` to configure how the server should connect to the database.
