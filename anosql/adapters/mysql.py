from contextlib import contextmanager

from anosql.patterns import var_pattern

def replacer(match):
    gd = match.groupdict()
    if gd['dblquote'] is not None:
        return gd['dblquote']
    elif gd['quote'] is not None:
        return gd["quote"]
    else:
        return '{lead}%({var_name})s{trail}'.format(
            lead=gd['lead'],
            var_name=gd['var_name'],
            trail=gd['trail'],
        )

class MySQLDriverAdapter(object):
    @staticmethod
    def process_sql(_query_name, _op_type, sql):
        return var_pattern.sub(replacer, sql)
    
    @staticmethod
    def select(conn, _query_name, sql, parameters):
        cur = conn.cursor()
        cur.execute(sql, parameters)
        results = cur.fetchall()
        cur.close()
        return results

    @staticmethod
    @contextmanager
    def select_cursor(conn, _query_name, sql, parameters):
        cur = conn.cursor()
        cur.execute(sql, parameters)
        try:
            yield cur
        finally:
            cur.close()

    @staticmethod
    def insert_update_delete(conn, _query_name, sql, parameters):
        cur = conn.cursor()
        try:
            cur.execute(sql, parameters)
        finally:
            cur.close()

    @staticmethod
    def insert_update_delete_many(conn, _query_name, sql, parameters):
        cur = conn.cursor()
        try:
            cur.executemany(sql, parameters)
        finally:
            cur.close()

    @staticmethod
    def insert_returning(conn, _query_name, sql, parameters):
        cur = conn.cursor()
        cur.execute(sql, parameters)
        results = cur.lastrowid
        cur.close()
        return results

    @staticmethod
    def execute_script(conn, sql):
        conn.executescript(sql)
