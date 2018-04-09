"use strict";

const Mysql = require("mysql");

const KOCString = require("koc-common-string");
const KOCReturn = require("koc-common-return");

let poolCluster = null;
let cacheRedis = null;

const KOCMysql = {
  /********************************
   * Init 初始化
   ********************************/
  Init: (dblist, redis, clear) => {
    if (poolCluster) return KOCMysql;
    poolCluster = Mysql.createPoolCluster();
    dblist.forEach((ThisValue) => {
      poolCluster.add(ThisValue.name, ThisValue);
    });
    cacheRedis = redis;
    if (cacheRedis && clear) KOCMysql.CacheClear();
    return KOCMysql;
  },
  /********************************
   * Conn 初始化
   ********************************/
  Conn: (dbname) => {
    return new Promise((resolve) => {
      poolCluster.getConnection(dbname, (err, conn) => {
        const retValue = KOCReturn.Value();
        if (err) {
          //记录日志
          console.error("连接DB出错[" + dbname + "]");
          console.error(err.message);
          retValue.hasError = true;
          retValue.errorCode = "Conn Error";
          return resolve(retValue);
        }
        conn.config.queryFormat = function (query, values) {
          if (!values) {
            return query;
          }
          if (values instanceof Array || typeof values !== "object") {
            return Mysql.format(query, values);
          }
          return query.replace(/\:(\w+)/g, function (txt, key) {
            if (values.hasOwnProperty(key)) {
              return conn.escape(values[key]);
            }
            return txt;
          }.bind(this));
        };
        retValue.returnObject = conn;
        resolve(retValue);
      });
    });
  },
  /********************************
   * Query 查询
   ********************************/
  Query: (dbconn, sql, parm, cache) => {
    return new Promise(async (resolve) => {
      let conn, tran = false;
      let retValue = KOCReturn.Value();
      // 判断事务
      if ((typeof dbconn !== 'string') || (dbconn.constructor !== String)) {
        tran = true;
        conn = dbconn;
      }
      // 缓存操作
      if (cacheRedis && !tran) {
        if (!cache) {
          // 强制清除缓存
          KOCMysql.CacheRemove(dbconn, sql, parm);
        } else {
          // 读取缓存数据
          retValue.returnObject = await KOCMysql.CacheGet(dbconn, sql, parm);
          if (retValue.returnObject) return resolve(retValue);
        }
      }
      // 取得连接
      if (!conn) {
        retValue = await KOCMysql.Conn(dbconn);
        if (retValue.hasError) return resolve(retValue);
        conn = retValue.returnObject;
      }
      // 打印SQL
      // console.log(conn.config.queryFormat(sql, parm));
      conn.query(sql, parm, (err, rows) => {
        if (!tran) conn.release();
        if (err) {
          //记录日志
          console.error("查询出错[" + sql + "] 错误信息[" + err + "]");
          console.error(parm);
          console.error(err.message);
          retValue.hasError = true;
          retValue.message = err.message;
          return resolve(retValue);
        }
        retValue.returnObject = rows;
        //写入缓存
        if (cache && !tran && cacheRedis && retValue.returnObject instanceof Array && retValue.returnObject.length) KOCMysql.CachePut(dbconn, sql, parm, retValue.returnObject, cache);
        resolve(retValue);
      });
    });
  },
  /********************************
   * ExecuteTable 查询列表
   ********************************/
  ExecuteTable: (dbconn, sql, parm, cache) => {
    return new Promise(async (resolve) => {
      const retValue = await KOCMysql.Query(dbconn, sql, parm, cache);
      if (retValue.hasError) return resolve(retValue);
      if (!(retValue.returnObject instanceof Array)) {
        retValue.hasError = true;
        return resolve(retValue);
      }
      resolve(retValue);
    });
  },
  ExecuteTableCache: async (dbconn, sql, parm, cache) => {
    return await KOCMysql.ExecuteTable(dbconn, sql, parm, cache || true);
  },
  /********************************
   * ExecuteRow 查询行
   ********************************/
  ExecuteRow: (dbconn, sql, parm, cache) => {
    return new Promise(async (resolve) => {
      const retValue = await KOCMysql.ExecuteTable(dbconn, sql, parm, cache);
      if (!retValue.hasError) {
        if (retValue.returnObject.length <= 0) {
          retValue.returnObject = null;
        } else {
          retValue.returnObject = retValue.returnObject[0];
        }
      }
      resolve(retValue);
    });
  },
  ExecuteRowCache: async (dbconn, sql, parm, cache) => {
    return await KOCMysql.ExecuteRow(dbconn, sql, parm, cache || true);
  },
  /********************************
   * ExecuteNonQuery 执行，返回受影响的行
   ********************************/
  ExecuteNonQuery: (dbconn, sql, parm, cacheRemove, cacheDBName) => {
    return new Promise(async (resolve) => {
      const retValue = await KOCMysql.Query(dbconn, sql, parm, false);
      if (retValue.hasError) return resolve(retValue);
      let insertID;
      let affectedRows = 0;
      if (retValue.returnObject instanceof Array) {
        insertID = [];
        for (const thisValue of retValue.returnObject) {
          if (!thisValue.hasOwnProperty('affectedRows') || !thisValue.hasOwnProperty('insertId')) {
            retValue.hasError = true;
            return resolve(retValue);
          }
          if (thisValue.insertId) insertID.push(thisValue.insertId);
          affectedRows += this.affectedRows;
        }
        if (insertID.length) insertID = null;
      } else {
        insertID = retValue.returnObject.insertId;
        affectedRows = retValue.returnObject.affectedRows;
      }
      if (cacheRemove) KOCMysql.CacheRemoveList(cacheRemove, cacheDBName, insertID);
      retValue.PutValue("insertId", insertID);
      retValue.returnObject = affectedRows;
      resolve(retValue);
    });
  },
  /********************************
   * TranOpen 事务开启
   ********************************/
  TranOpen: (db) => {
    return new Promise(async (resolve) => {
      const retValue = await KOCMysql.Conn(db);
      if (retValue.hasError) return resolve(retValue);
      retValue.returnObject.beginTransaction((err) => {
        if (err) {
          retValue.hasError = true;
          retValue.message = err.message;
          return resolve(retValue);
        }
        resolve(retValue);
      });
    });
  },
  /********************************
   * TranRollback 事务会滚
   ********************************/
  TranRollback: (conn) => {
    return new Promise((resolve) => {
      if (!conn) return resolve();
      conn.rollback(function () {
        conn.release();
        resolve();
      });
    });
  },
  /********************************
   * TranCommit 事务提交
   ********************************/
  TranCommit: (conn) => {
    return new Promise((resolve) => {
      const retValue = KOCReturn.Value();
      if (!conn) {
        retValue.hasError = true;
        retValue.message = "空连接不能提交事务";
        return resolve(retValue);
      }
      conn.commit(async (err) => {
        if (err) {
          retValue.hasError = true;
          retValue.message = err.message;
          await KOCMysql.TranRollback(conn);
          return resolve(retValue);
        }
        conn.release();
        resolve(retValue);
      });
    });
  },
  /********************************
   * AddToWhereSQL 添加条件
   ********************************/
  AddToWhereSQL: (whereSQL, addSQL, opSQL) => {
    whereSQL = KOCString.ToString(whereSQL).trim();
    if (whereSQL) {
      whereSQL += " " + opSQL + " (" + addSQL + ") ";
    } else {
      whereSQL = " (" + addSQL + ") ";
    }
    return whereSQL.trim();
  },
  /********************************
   * ToDBStr
   ********************************/
  ToDBStr: (str) => {
    return KOCString.ToString(str).replace(/'/g, "''").replace(/`/g, " ");
  },
  /********************************
   * PageParm 分页，参数
   ********************************/
  PageParm: function () {
    this.GetPageInfo = true;
    this.ColumnPK = "";
    this.ColumnMAX = "";
    this.ColumnList = "";
    this.TableList = "";
    this.Condition = "";
    this.OrderName = "";
    this.Start = 1;
    this.Length = 0;
  },
  /********************************
   * PageInfo 分页，页数据
   ********************************/
  PageInfo: async (db, pageparm, parm) => {
    const sql = "SELECT COUNT(" + KOCMysql.ToDBStr(pageparm.ColumnPK) + ") AS `RecordCount`, MAX(" + KOCMysql.ToDBStr(pageparm.ColumnMAX) + ") AS `MaxCode`" +
      " FROM " + pageparm.TableList
      + (pageparm.Condition ? (" WHERE " + pageparm.Condition) : "");
    const retValue = await KOCMysql.ExecuteRow(db, sql, parm);
    if (retValue.hasError) {
      retValue.hasError = false;
      retValue.returnObject = {
        RecordCount: 0,
        MaxCode: ""
      };
    }
    return retValue;
  },
  /********************************
   * PageList 分页，列表
   ********************************/
  PageList: async (db, pageparm, parm) => {
    const sql = "SELECT " + pageparm.ColumnList
      + " FROM " + pageparm.TableList
      + (pageparm.Condition ? (" WHERE " + pageparm.Condition) : "")
      + (pageparm.OrderName ? (" ORDER BY " + KOCMysql.ToDBStr(pageparm.OrderName)) : "")
      + " LIMIT " + pageparm.Start + ", " + pageparm.Length;
    return await KOCMysql.ExecuteTable(db, sql, parm);
  },
  /********************************
   * PageList 分页
   ********************************/
  Page: async (db, pageparm, parm) => {
    let retValue = await KOCMysql.PageList(db, pageparm, parm);
    if (!pageparm.GetPageInfo || retValue.hasError) return retValue;
    retValue.PutValue("PageInfo", (await KOCMysql.PageInfo(db, pageparm, parm)).returnObject);
    return retValue;
  },
  /********************************
   * CachePut 缓存写入
   ********************************/
  CachePut: function (dbname, sql, parm, object, expire) {
    if (!cacheRedis || !object) return;
    cacheRedis.set(KOCMysql.CacheKey(dbname, sql, parm), JSON.stringify(object), "EX", KOCMysql.CacheExpire(expire));
  },
  /********************************
   * CacheGet 缓存取出
   ********************************/
  CacheGet: (dbname, sql, parm) => {
    return new Promise((resolve) => {
      if (!cacheRedis) return resolve();
      cacheRedis.get(KOCMysql.CacheKey(dbname, sql, parm), function (err, result) {
        if (err || !result) return resolve();
        try {
          result = JSON.parse(result);
        } catch (ex) {
        }
        resolve(result);
      });
    });
  },
  /********************************
   * CacheRemove 缓存移除
   ********************************/
  CacheRemove: function (dbname, sql, parm) {
    if (!cacheRedis) return;
    cacheRedis.del(KOCMysql.CacheKey(dbname, sql, parm));
  },
  CacheRemoveList: function (value, dbname, insertId) {
    if (!cacheRedis) return;
    if (!(value instanceof Array)) value = [value];
    for (const thisValue of value) {
      const cacheRemoveList = [];
      if (typeof thisValue !== "function") {
        cacheRemoveList.push(thisValue)
      } else if (insertId) {
        if (!(insertId instanceof Array)) insertId = [insertId];
        for (const thisInsertID of insertId) {
          cacheRemoveList.push(thisValue(thisInsertID))
        }
      }
      for (const thisCacheRemove of cacheRemoveList) {
        try {
          KOCMysql.CacheRemove(dbname || thisCacheRemove.DB, thisCacheRemove.SQL, thisCacheRemove.Parm);
        } catch (ex) {
        }
      }
    }
  },
  /********************************
   * CacheClear 缓存清所(所有缓存:慎用)
   ********************************/
  CacheClear: () => {
    if (!cacheRedis) return;
    cacheRedis.flushdb();
  },
  /********************************
   * CacheKey 缓存Key
   ********************************/
  CacheKey: (dbname, sql, parm) => {
    return KOCString.MD5(KOCString.ToString(dbname) + KOCString.ToString(sql) + JSON.stringify(parm));
  },
  /********************************
   * CacheExpire 缓存过期时间(分钟)默认3分钟
   ********************************/
  CacheExpire: (expire) => {
    return KOCString.ToIntPositive(expire, 3) * 60;
  }
};

module.exports = KOCMysql;