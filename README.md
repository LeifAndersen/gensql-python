# GenSQL for Python

This package provides bindings for [GenSQL](https://arxiv.org/abs/2406.15652).

This package requires Java (>= 24) to be installed, either in `$JAVA_HOME`, or in your `$PATH`.

## Usage:

First, you'll need a database file. You can find an example one [here](https://github.com/LeifAndersen/gensql-python/blob/main/tests/db.edn) or create one using your own data set with [this pipeline](https://github.com/LeifAndersen/GenSQL.structure-learning/tree/dstop2)

Then, in your python file, you can load and query your database like so:

```
import gensql

db = gensql.DB("db.edn")
db.query("SELECT * FROM data LIMIT 5")
```

Optionally you can pick between the `strict` and `permissive` variants of gensql with the `mode` flag (defaults to `permissive`):

```
db.query("SELECT * FROM data LIMIT 5", mode="permissive")
```

## Known Limitations

For the moment, only one Python process can use GenSQL at a time. Starting up a second one will hang until the first one finishes.
