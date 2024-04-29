import pymysql,time
from pymysql import converters
from dbutils.pooled_db import PooledDB

MYSQL_CONF = {
        "host": "172.30.224.1",
        "port": 3306,
        "user": "root",
        "password": "root",
        "database": "demo"
    }

class DbUtil:
    __converions = converters.conversions
    __converions[pymysql.FIELD_TYPE.BIT] = lambda x: b'1' if x == b'\x01' else b'0'

    def __init__(self):
        self.pool = PooledDB(
            creator=pymysql,  
            mincached=10,  
            maxconnections=200,  
            blocking=True,  
            charset='utf8',
            conv=self.__converions,
           **MYSQL_CONF
        )

    def __open(self):
        self.conn = self.pool.connection()
        self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)  
        return self.conn, self.cursor
    
    def __close(self, cursor, conn):
        cursor.close()
        conn.close()

    def __executeWithArgs(self, sql, args, isNeed=False):
        conn, cursor = self.__open()
        if isNeed:
            try:
                cursor.execute(sql, args)
                conn.commit()
            except:
                conn.rollback()
        else:
            cursor.execute(sql, args)
            conn.commit()
        self.__close(conn, cursor)

    def __execute(self, sql, isNeed=False):
        conn, cursor = self.__open()
        if isNeed:
            try:
                cursor.execute(sql)
                conn.commit()
            except Exception as err:
                conn.rollback()
                raise err.args
        else:
            cursor.execute(sql)
            conn.commit()
        self.__close(conn, cursor)

    def __batch_execute(self,sql,values):
        conn, cursor = self.__open()
        try:
            cursor.executemany(sql, values)
            conn.commit()
            return cursor.rowcount
        except Exception as err:
            conn.rollback()
            raise err.args
        finally:
            self.__close(conn, cursor)

    def query(self,sql):
        conn, cursor = self.__open()
        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            return result
        except BaseException as e:
            raise e.args
        finally:
            self.__close(conn, cursor)
    
    def update(self,sql):
        self.__execute(sql, isNeed=True)

    def insert(self,sql):
        conn, cursor = self.__open()
        try:
            cursor.execute(sql)
            conn.commit()
            return cursor.lastrowid
        except BaseException as e:
            conn.rollback()
            raise e.args
        finally:
            self.__close(conn, cursor)
        
   
    def batch_insert(self,sql,values):
        return self.__batch_execute(sql,values)

    def batch_update(self,sql,values):
        return self.__batch_execute(sql,values)
    
    def delete(self,sql):
        self.__execute(sql, isNeed=True)

if(__name__ == '__main__'):
    print('start ')
    sql = 'select vin from vehicle  where id < 100 limit 1'
    db = DbUtil()
    res = db.query(sql)
    print(res)
    print('end')

   

