import sqlite3


def get_tables_names(db):
    with sqlite3.connect(db) as conn:
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return c.fetchall()


def select_all(table_name, db):
    with sqlite3.connect(db) as conn:
        c = conn.cursor()
        c.execute(" SELECT * FROM {} ".format(table_name))
        return c.fetchall()

def select_where(table_name, column, value, db):
    with sqlite3.connect(db) as conn:
        c = conn.cursor()
        c.execute(" SELECT * FROM {} WHERE {} = '{}' ".format(table_name, column, value))
        return c.fetchall()


def select_where_both(table_name, column1, value1, column2, value2, db):
    with sqlite3.connect(db) as conn:
        c = conn.cursor()
        c.execute(" SELECT * FROM {} WHERE {} = {} and {} = {} ".format(table_name, column1, value1, column2, value2))
        return c.fetchall()


#list of tuples [("col_name", "type"),...]
def create_table_list(table_name, col_type_pairs, db):
    with sqlite3.connect(db) as conn:
        c = conn.cursor()
        s = ""
        for pair in col_type_pairs:
            p = " ".join(pair)
            s += p + ", "
        s = s[:-2]
        c.execute(" CREATE TABLE {} ( {} )".format(table_name, s))
        conn.commit()
        return

def create_table_string(table_name, col_type_pairs, db):
    with sqlite3.connect(db) as conn:
        c = conn.cursor()
        c.execute(" CREATE TABLE {} ( {} )".format(table_name, col_type_pairs))
        conn.commit()
        return


def drop_table(table_name, db):
    with sqlite3.connect(db) as conn:
        c = conn.cursor()
        c.execute(" DROP TABLE {} ".format(table_name))
        conn.commit()
        return

def delete_table(table_name, db):
    with sqlite3.connect(db) as conn:
        c = conn.cursor()
        c.execute(" DELETE FROM {}  ".format(table_name))
        conn.commit()
        return

def rename_table(table_name, new_name, db):
    with sqlite3.connect(db) as conn:
        c = conn.cursor()
        c.execute("ALTER TABLE {} RENAME TO {}".format(table_name, new_name))

def add_column(table_name, name, var_type, db):
    with sqlite3.connect(db) as conn:
        c = conn.cursor()
        c.execute("ALTER TABLE {} ADD {} {}".format(table_name, name, var_type))
        conn.commit()
        return

def insert_into(table_name, row, db):
    with sqlite3.connect(db) as conn:
        c = conn.cursor()
        c.execute("""INSERT INTO {} VALUES{} """.format(table_name, row))
        conn.commit()
        return

def insert_into_by_col(table_name, row, col_order, db):
    with sqlite3.connect(db) as conn:
        c = conn.cursor()
        c.execute("""INSERT INTO {} {} VALUES{} """.format(table_name, col_order ,row))
        conn.commit()
        return


def insert_rows(table, rows, db):
    with sqlite3.connect(db) as conn:
        c = conn.cursor()
        num_columns = len(rows[0])
        s1 = "?, " * num_columns
        s = "(" + s1[:-2] + ")"
        c.executemany(""" INSERT INTO {}  VALUES {} """.format(table, s), rows)
        conn.commit()
        return

def insert_rows_by_col(table, rows, col_order, db):
    with sqlite3.connect(db) as conn:
        c = conn.cursor()
        num_columns = len(rows[0])
        s1 = "?, " * num_columns
        s = "(" + s1[:-2] + ")"
        c.executemany(""" INSERT INTO {} {} VALUES {} """.format(table, col_order , s), rows)
        conn.commit()
        return

#def insert_if_not_exist():
    """INSERT INTO {}(id,text)
SELECT {}
WHERE NOT EXISTS(SELECT 1 FROM memos WHERE id = 5 AND text = 'text to insert');
    """
def update_column(table_name, column, new_val, db):
    with sqlite3.connect(db) as conn:
        c = conn.cursor()
        c.execute("UPDATE {} SET {}={} ".format(table_name, column,new_val))
        conn.commit()
        return

def update_column_where(table_name, column, new_val, where_expression, db):
    with sqlite3.connect(db) as conn:
        c = conn.cursor()
        c.execute("UPDATE {} SET {} = '{}' {} ".format(table_name, column, new_val, where_expression))
        conn.commit()
        return
