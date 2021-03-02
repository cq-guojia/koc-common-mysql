'use strict'

const Mysql = require('mysql')

const KOCString = require('koc-common-string')
const KOCReturn = require('koc-common-return/index')

let poolCluster = null
let cacheRedis = null

const KOCMysql = module.exports = {
  /**
   * @desc 初始化
   * @param dblist
   * @param redis
   * @param clear
   * @return {any}
   */
  Init: (dblist, redis, clear) => {
    if (poolCluster) return KOCMysql
    poolCluster = {}
    dblist.forEach((thisValue) => {
      const name = thisValue.name
      delete thisValue.name
      poolCluster[name] = Mysql.createPool(thisValue)
    })
    cacheRedis = redis
    if (cacheRedis && clear) KOCMysql.CacheClear()
    return KOCMysql
  },
  /**
   * @desc 初始化连接
   * @param dbname
   * @return {Promise}
   */
  Conn: (dbname) => {
    return new Promise((resolve) => {
      poolCluster[dbname].getConnection((err, conn) => {
        const retValue = KOCReturn.Value()
        if (err) {
          //记录日志
          console.error('连接DB出错[' + dbname + ']')
          console.error(err.message)
          retValue.hasError = true
          retValue.errorCode = 'Conn Error'
          return resolve(retValue)
        }
        conn.config.queryFormat = function (query, values) {
          if (!values) {
            return query
          }
          if (values instanceof Array || typeof values !== 'object') {
            return Mysql.format(query, values)
          }
          return query.replace(/\:(\w+)/g, function (txt, key) {
            if (values.hasOwnProperty(key)) {
              return conn.escape(values[key])
            }
            return txt
          }.bind(this))
        }
        retValue.returnObject = conn
        resolve(retValue)
      })
    })
  },
  /**
   * @desc 执行查询
   * @param dbconn
   * @param sql
   * @param parm
   * @param cache
   * @return {Promise}
   */
  Query: (dbconn, sql, parm, cache) => {
    return new Promise(async (resolve) => {
      let conn, tran = false
      let retValue = KOCReturn.Value()
      // 判断事务
      if ((typeof dbconn !== 'string') || (dbconn.constructor !== String)) {
        tran = true
        conn = dbconn
      }
      // 缓存操作
      if (cacheRedis && !tran) {
        if (!cache) {
          // 强制清除缓存
          KOCMysql.CacheRemove(dbconn, sql, parm)
        } else {
          // 读取缓存数据
          retValue.returnObject = await KOCMysql.CacheGet(dbconn, sql, parm)
          if (retValue.returnObject) return resolve(retValue)
        }
      }
      // 取得连接
      if (!conn) {
        retValue = await KOCMysql.Conn(dbconn)
        if (retValue.hasError) return resolve(retValue)
        conn = retValue.returnObject
      }
      // 打印SQL
      // console.log(conn.config.queryFormat(sql, parm))
      conn.query(sql, parm, (err, rows) => {
        if (!tran) conn.release()
        if (err) {
          //记录日志
          console.error('查询出错[' + sql + '] 错误信息[' + err + ']')
          console.error(parm)
          console.error(err.message)
          retValue.hasError = true
          retValue.message = err.message
          return resolve(retValue)
        }
        retValue.returnObject = rows
        //写入缓存
        if (cache && !tran && cacheRedis && Array.isArray(retValue.returnObject) && retValue.returnObject.length) KOCMysql.CachePut(dbconn, sql, parm, retValue.returnObject, cache)
        resolve(retValue)
      })
    })
  },
  /**
   * @desc 写入一条记录
   * @param dbconn
   * @param table
   * @param parm
   * @param [cacheRemove]
   * @param [cacheDBName]
   * @return {KOCReturn|Promise}
   */
  Insert: (dbconn, table, parm, cacheRemove, cacheDBName) => {
    if (!dbconn || !table || !parm) return KOCReturn.Value({ hasError: true, message: 'arguments error.' })
    const col = Object.keys(parm).map(thisValue => `\`${thisValue}\``).join(',')
    const val = Object.keys(parm).map(thisValue => `:${thisValue}`).join(',')
    let sql = `INSERT INTO ${table} (${col}) VALUES (${val});`
    return KOCMysql.ExecuteNonQuery(dbconn, sql, parm, cacheRemove, cacheDBName)
  },
  /**
   * @desc 写入或者更新
   * @param dbconn
   * @param table
   * @param insert
   * @param update
   * @param [cacheRemove]
   * @param [cacheDBName]
   * @return {KOCReturn|Promise}
   */
  InsertOrUpdate: (dbconn, table, insert, update, cacheRemove, cacheDBName) => {
    if (!dbconn || !table || !insert || !update) return KOCReturn.Value({ hasError: true, message: 'arguments error.' })
    const col = Object.keys(insert).map(thisValue => `\`${thisValue}\``).join(',')
    const val = Object.keys(insert).map(thisValue => `:${thisValue}`).join(',')
    const sqlUpdate = Object.keys(update).map(thisValue => `\`${thisValue}\` = :${thisValue}`).join(',')
    let sql = `INSERT INTO ${table} (${col}) VALUES (${val}) ON DUPLICATE KEY UPDATE ${sqlUpdate};`
    const parm = Object.assign({}, insert, update)
    return KOCMysql.ExecuteNonQuery(dbconn, sql, parm, cacheRemove, cacheDBName)
  },
  /**
   * @desc 更新
   * @param dbconn
   * @param table
   * @param update
   * @param condition
   * @param [cacheRemove]
   * @param [cacheDBName]
   * @return {KOCReturn|Promise}
   */
  Update: (dbconn, table, update, condition, cacheRemove, cacheDBName) => {
    if (!dbconn || !table || !update || !condition) return KOCReturn.Value({ hasError: true, message: 'arguments error.' })
    const sqlUpdate = Object.keys(update).map(thisValue => `\`${thisValue}\` = :${thisValue}`).join(',')
    const sqlWhere = Object.keys(condition).map(thisValue => `\`${thisValue}\` = :WHERE_${thisValue}`).join(' AND ')
    const parmWhere = {}
    Object.keys(condition).forEach(thisValue => { parmWhere['WHERE_' + thisValue] = condition[thisValue] })
    const parm = Object.assign({}, update, parmWhere)
    const sql = `UPDATE ${table} SET ${sqlUpdate} WHERE ${sqlWhere};`
    return KOCMysql.ExecuteNonQuery(dbconn, sql, parm, cacheRemove, cacheDBName)
  },
  /**
   * @desc 更新或者新建
   * @param dbconn
   * @param table
   * @param insert
   * @param update
   * @param condition
   * @param [cacheRemove]
   * @param [cacheDBName]
   * @return {Promise<KOCReturn|Promise>}
   */
  UpdateOrCreate: async (dbconn, table, insert, update, condition, cacheRemove, cacheDBName) => {
    if (!dbconn || !table || !insert || !update) return KOCReturn.Value({ hasError: true, message: 'arguments error.' })
    let retValue = await KOCMysql.Update(dbconn, table, update, condition, cacheRemove, cacheDBName)
    if (retValue.hasError) return retValue
    if (retValue.returnObject > 0) return retValue
    return KOCMysql.Insert(dbconn, table, insert, cacheRemove, cacheDBName)
  },
  /**
   * @desc 查询列表
   * @param dbconn
   * @param sql
   * @param parm
   * @param cache
   * @return {Promise}
   */
  ExecuteTable: (dbconn, sql, parm, cache) => {
    return new Promise(async (resolve) => {
      const retValue = await KOCMysql.Query(dbconn, sql, parm, cache)
      if (retValue.hasError) return resolve(retValue)
      if (!Array.isArray(retValue.returnObject)) {
        retValue.hasError = true
        return resolve(retValue)
      }
      resolve(retValue)
    })
  },
  /**
   * @desc 查询列表(缓存)
   * @param dbconn
   * @param sql
   * @param parm
   * @param cache
   * @return {Promise}
   */
  ExecuteTableCache: (dbconn, sql, parm, cache) => {
    return KOCMysql.ExecuteTable(dbconn, sql, parm, cache || true)
  },
  /**
   * @desc 查询一条记录
   * @param dbconn
   * @param sql
   * @param parm
   * @param cache
   * @return {Promise}
   */
  ExecuteRow: (dbconn, sql, parm, cache) => {
    return new Promise(async (resolve) => {
      const retValue = await KOCMysql.ExecuteTable(dbconn, sql, parm, cache)
      if (!retValue.hasError) {
        if (retValue.returnObject.length <= 0) retValue.returnObject = null
        else retValue.returnObject = retValue.returnObject[0]
      }
      resolve(retValue)
    })
  },
  /**
   * @desc 查询一条记录(缓存)
   * @param dbconn
   * @param sql
   * @param parm
   * @param cache
   * @return {Promise}
   */
  ExecuteRowCache: (dbconn, sql, parm, cache) => {
    return KOCMysql.ExecuteRow(dbconn, sql, parm, cache || true)
  },
  /**
   * @desc 执行语句返回受影响的行数
   * @param dbconn
   * @param sql
   * @param parm
   * @param cacheRemove
   * @param cacheDBName
   * @return {Promise}
   */
  ExecuteNonQuery: (dbconn, sql, parm, cacheRemove, cacheDBName) => {
    return new Promise(async (resolve) => {
      const retValue = await KOCMysql.Query(dbconn, sql, parm, false)
      if (retValue.hasError) return resolve(retValue)
      let insertID
      let affectedRows = 0
      if (Array.isArray(retValue.returnObject)) {
        insertID = []
        for (const thisValue of retValue.returnObject) {
          if (!thisValue.hasOwnProperty('affectedRows') || !thisValue.hasOwnProperty('insertId')) {
            retValue.hasError = true
            return resolve(retValue)
          }
          if (thisValue.insertId) insertID.push(thisValue.insertId)
          affectedRows += this.affectedRows
        }
        if (insertID.length) insertID = null
      } else {
        insertID = retValue.returnObject.insertId
        affectedRows = retValue.returnObject.affectedRows
      }
      if (cacheRemove) KOCMysql.CacheRemoveList(cacheRemove, cacheDBName, insertID)
      retValue.PutValue('insertId', insertID)
      retValue.returnObject = affectedRows
      resolve(retValue)
    })
  },
  /**
   * @desc 事务开启
   * @param db
   * @return {Promise}
   */
  TranOpen: (db) => {
    return new Promise(async (resolve) => {
      const retValue = await KOCMysql.Conn(db)
      if (retValue.hasError) return resolve(retValue)
      retValue.returnObject.beginTransaction((err) => {
        if (err) {
          retValue.hasError = true
          retValue.message = err.message
          return resolve(retValue)
        }
        resolve(retValue)
      })
    })
  },
  /**
   * @desc 事务回滚
   * @param conn
   * @return {Promise}
   */
  TranRollback: (conn) => {
    return new Promise((resolve) => {
      if (!conn) return resolve()
      conn.rollback(function () {
        conn.release()
        resolve()
      })
    })
  },
  /**
   * @desc 事务提交
   * @param conn
   * @return {Promise}
   */
  TranCommit: (conn) => {
    return new Promise((resolve) => {
      const retValue = KOCReturn.Value()
      if (!conn) {
        retValue.hasError = true
        retValue.message = '空连接不能提交事务'
        return resolve(retValue)
      }
      conn.commit(async (err) => {
        if (err) {
          retValue.hasError = true
          retValue.message = err.message
          await KOCMysql.TranRollback(conn)
          return resolve(retValue)
        }
        conn.release()
        resolve(retValue)
      })
    })
  },
  /**
   * @desc 添加条件
   * @param whereSQL
   * @param addSQL
   * @param opSQL
   * @return {string}
   */
  AddToWhereSQL: (whereSQL, addSQL, opSQL) => {
    whereSQL = KOCString.ToString(whereSQL).trim()
    if (whereSQL) {
      whereSQL += ' ' + opSQL + ' (' + addSQL + ') '
    } else {
      whereSQL = ' (' + addSQL + ') '
    }
    return whereSQL.trim()
  },
  /**
   * @desc 语句防注入处理
   * @param str
   * @return {string}
   */
  ToDBStr: (str) => KOCString.ToString(str).replace(/'/g, '\'\'').replace(/`/g, ' '),
  /**
   * @desc 分页，参数
   */
  PageParm: function () {
    this.GetPageInfo = true
    this.ColumnPK = ''
    this.ColumnMAX = ''
    this.ColumnList = ''
    this.TableList = ''
    this.Condition = ''
    this.OrderName = ''
    this.Start = 1
    this.Length = 0
  },
  /**
   * @desc 分页，页数据
   * @param db
   * @param pageparm
   * @param pageparm.ColumnPK
   * @param pageparm.ColumnMAX
   * @param pageparm.TableList
   * @param pageparm.Condition
   * @param parm
   * @return {Promise}
   */
  PageInfo: async (db, pageparm, parm) => {
    const sql = 'SELECT COUNT(' + KOCMysql.ToDBStr(pageparm.ColumnPK) + ') AS `RecordCount`, MAX(' + KOCMysql.ToDBStr(pageparm.ColumnMAX) + ') AS `MaxCode`' +
      ' FROM ' + pageparm.TableList
      + (pageparm.Condition ? (' WHERE ' + pageparm.Condition) : '')
    const retValue = await KOCMysql.ExecuteRow(db, sql, parm)
    if (retValue.hasError) {
      retValue.hasError = false
      retValue.returnObject = { RecordCount: 0, MaxCode: '' }
    }
    return retValue
  },
  /**
   * @desc 分页，列表
   * @param db
   * @param pageparm
   * @param pageparm.ColumnList
   * @param pageparm.TableList
   * @param pageparm.Condition
   * @param pageparm.OrderName
   * @param pageparm.Start
   * @param pageparm.Length
   * @param parm
   * @return {Promise}
   */
  PageList: (db, pageparm, parm) => {
    const sql = 'SELECT ' + pageparm.ColumnList
      + ' FROM ' + pageparm.TableList
      + (pageparm.Condition ? (' WHERE ' + pageparm.Condition) : '')
      + (pageparm.OrderName ? (' ORDER BY ' + KOCMysql.ToDBStr(pageparm.OrderName)) : '')
      + ' LIMIT ' + pageparm.Start + ', ' + pageparm.Length
    return KOCMysql.ExecuteTable(db, sql, parm)
  },
  /**
   * @desc 分页
   * @param db
   * @param pageparm
   * @param parm
   * @return {Promise}
   */
  Page: async (db, pageparm, parm) => {
    let retValue = await KOCMysql.PageList(db, pageparm, parm)
    if (!pageparm.GetPageInfo || retValue.hasError) return retValue
    retValue.PutValue('PageInfo', (await KOCMysql.PageInfo(db, pageparm, parm)).returnObject)
    return retValue
  },
  /**
   * @desc 缓存写入
   * @param dbname
   * @param sql
   * @param parm
   * @param object
   * @param expire
   */
  CachePut: function (dbname, sql, parm, object, expire) {
    if (!cacheRedis || !object) return
    cacheRedis.set(KOCMysql.CacheKey(dbname, sql, parm), JSON.stringify(object), 'EX', KOCMysql.CacheExpire(expire))
  },
  /**
   * @desc 缓存取出
   * @param dbname
   * @param sql
   * @param parm
   * @return {Promise}
   */
  CacheGet: (dbname, sql, parm) => {
    return new Promise((resolve) => {
      if (!cacheRedis) return resolve()
      cacheRedis.get(KOCMysql.CacheKey(dbname, sql, parm), function (err, result) {
        if (err || !result) return resolve()
        try {
          result = JSON.parse(result)
        } catch (ex) {
        }
        resolve(result)
      })
    })
  },
  /**
   * @desc 缓存移除
   * @param dbname
   * @param sql
   * @param parm
   */
  CacheRemove: function (dbname, sql, parm) {
    if (!cacheRedis) return
    cacheRedis.del(KOCMysql.CacheKey(dbname, sql, parm))
  },
  /**
   * @desc 缓存移除(批量)
   * @param value
   * @param dbname
   * @param insertId
   */
  CacheRemoveList: function (value, dbname, insertId) {
    if (!cacheRedis) return
    if (!(value instanceof Array)) value = [value]
    for (const thisValue of value) {
      const cacheRemoveList = []
      if (typeof thisValue !== 'function') {
        cacheRemoveList.push(thisValue)
      } else if (insertId) {
        if (!(insertId instanceof Array)) insertId = [insertId]
        for (const thisInsertID of insertId) {
          cacheRemoveList.push(thisValue(thisInsertID))
        }
      }
      for (const thisCacheRemove of cacheRemoveList) {
        try {
          KOCMysql.CacheRemove(dbname || thisCacheRemove.DB, thisCacheRemove.SQL, thisCacheRemove.Parm)
        } catch (ex) {
        }
      }
    }
  },
  /**
   * @desc 缓存清空(所有缓存:慎用)
   */
  CacheClear: () => {
    if (!cacheRedis) return
    cacheRedis.flushdb()
  },
  /**
   * @desc 缓存Key
   * @param dbname
   * @param sql
   * @param parm
   * @return {*}
   */
  CacheKey: (dbname, sql, parm) => KOCString.MD5(KOCString.ToString(dbname) + KOCString.ToString(sql) + JSON.stringify(parm)),
  /**
   * @desc 缓存过期时间(分钟)默认3分钟
   * @param expire
   * @return {number}
   */
  CacheExpire: (expire) => KOCString.ToIntPositive(expire, 3) * 60
}
