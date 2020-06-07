const fs = require('fs')
const path = require('path')
const mysql = require('mysql2')
const util = require('util')

module.exports = {
  setup: async (storage, moduleName) => {
    const databaseURL = process.env[`${moduleName}_DATABASE_URL`] || process.env.DATABASE_URL || 'mysql://localhost:3306/testing'
    const pool = mysql.createPool(({ uri: databaseURL, multipleStatements: true }))
    const dashboardPath1 = path.join(global.applicationPath, 'node_modules/@userdashboard/dashboard/src/log.js')
    let Log
    if (fs.existsSync(dashboardPath1)) {
      Log = require(dashboardPath1)('mysql-list')
    } else {
      const dashboardPath2 = path.join(global.applicationPath, 'src/log.js')
      Log = require(dashboardPath2)('mysql-list')
    }
    const container = {
      add: util.promisify((path, objectid, callback) => {
        if (objectid === true || objectid === false) {
          objectid = objectid.toString()
        }
        return pool.query(mysql.format('INSERT INTO lists(path, objectid) VALUES (?, ?)', [path, objectid]), (error, result) => {
          if (error) {
            Log.error('error adding', error)
            return callback(new Error('unknown-error'))
          }
          return callback(null, result)
        })
      }),
      addMany: util.promisify((items, callback) => {
        const commands = []
        const values = []
        for (const path in items) {
          commands.push('INSERT INTO lists(path, objectid) VALUES (?, ?)')
          values.push(path, items[path])
        }
        return pool.query(commands.join('; '), values, (error) => {
          if (error) {
            Log.error('error adding many', error)
            return callback(new Error('unknown-error'))
          }
          return callback()
        })
      }),
      count: util.promisify((path, callback) => {
        return pool.query(mysql.format('SELECT COUNT(*) AS counter FROM lists WHERE path=?', [path]), (error, result) => {
          if (error) {
            Log.error('error counting', error)
            return callback(new Error('unknown-error'))
          }
          if (!result || !result.length || !result[0] || result[0].counter === undefined) {
            return callback(new Error('unknown-error'))
          }
          return callback(null, result[0].counter)
        })
      }),
      exists: util.promisify((path, objectid, callback) => {
        return pool.query(mysql.format('SELECT EXISTS(SELECT 1 FROM lists WHERE path=? AND objectid=?) AS item', [path, objectid]), (error, result) => {
          if (error) {
            Log.error('error checking exists', error)
            return callback(new Error('unknown-error'))
          }
          if (!result || !result.length || !result[0] || result[0].item === undefined) {
            return callback(new Error('unknown-error'))
          }
          return callback(null, result[0].item === 1)
        })
      }),
      list: util.promisify((path, offset, pageSize, callback) => {
        offset = offset || 0
        if (pageSize === null || pageSize === undefined) {
          pageSize = global.pageSize
        }
        if (offset < 0) {
          return callback(new Error('invalid-offset'))
        }
        return pool.query(mysql.format(`SELECT objectid FROM lists WHERE path=? ORDER BY id DESC LIMIT ${pageSize} OFFSET ${offset}`, [path]), (error, result) => {
          if (error) {
            Log.error('error listing', error)
            return callback(new Error('unknown-error'))
          }
          if (!result || !result.length) {
            return callback()
          }
          const data = []
          for (const row of result) {
            data.push(row.objectid)
          }
          return callback(null, data)
        })
      }),
      listAll: util.promisify((path, callback) => {
        return pool.query(mysql.format('SELECT objectid FROM lists WHERE path=? ORDER BY id DESC', [path]), (error, result) => {
          if (error) {
            Log.error('error listing all', error)
            return callback(new Error('unknown-error'))
          }
          if (!result || !result || !result.length) {
            return callback()
          }
          const data = []
          for (const row of result) {
            data.push(row.objectid)
          }
          return callback(null, data)
        })
      }),
      remove: util.promisify((_, objectid, callback) => {
        objectid = objectid.toString()
        return pool.query(mysql.format('DELETE FROM lists WHERE objectid=?', [objectid]), (error, result) => {
          if (error) {
            Log.error('error removing', error)
            return callback(new Error('unknown-error'))
          }
          if (!result) {
            return callback(new Error('unknown-error'))
          }
          return callback()
        })
      })
    }
    return container
  }
}
