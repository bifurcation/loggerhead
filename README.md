loggerhead
==========

A minimal CT log server focused on optimizing out merge delay time.  The server
will only return an SCT if a new tree head covering the certificate has already
been logged in the database.

The server relies on a database with the following two tables (using postgres
data types):

```
CREATE TABLE frontier (index BIGINT, subtree_size BIGINT, subhead BYTEA);
CREATE TABLE certificates (timestamp BIGINT, tree_size BIGINT, tree_head BYTEA, cert BYTEA);
```

Edit `db_config.go` to configure how the server should connect to the database.
