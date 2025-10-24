import pytest
import gensql

db_file = __file__ + "/../db.edn"

def test_db():
    db = gensql.DB(db_file)
    ret = db.query("SELECT Height FROM data LIMIT 1")
    assert ret == [{"Height": 59.26316254117439}]

def test_two_connections():
    db1 = gensql.DB(db_file)
    db2 = gensql.DB(db_file)
    assert db1.query("SELECT * FROM data LIMIT 10") == db2.query("SELECT * FROM data LIMIT 10")

def test_modes():
    db = gensql.DB(db_file)
    assert db.query("SELECT * FROM data LIMIT 10", mode="permissive") == db.query("SELECT * FROM data LIMIT 10", mode="strict")
    with pytest.raises(ValueError):
        db.query("SELECT * FROM data", mode="doesnotexist")