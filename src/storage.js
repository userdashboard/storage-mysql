const fs = require('fs')
const mysql = require('mysql2')
const path = require('path')
const util = require('util')

module.exports = {
  setup: util.promisify((moduleName, callback) => {
    if (!callback) {
      callback = moduleName
      moduleName = null
    }
    const databaseURL = process.env[`${moduleName}_DATABASE_URL`] || process.env.DATABASE_URL || 'mysql://localhost:3306/testing'
    let setupSQLFile = path.join(__dirname, 'setup.sql')
    if (!fs.existsSync(setupSQLFile)) {
      setupSQLFile = path.join(global.applicationPath, 'node_modules/@userdashboard/storage-mysql/setup.sql')
    }
    setupSQLFile = fs.readFileSync(setupSQLFile).toString()
    const dashboardPath1 = path.join(global.applicationPath, 'node_modules/@userdashboard/dashboard/src/log.js')
    let Log
    if (fs.existsSync(dashboardPath1)) {
      Log = require(dashboardPath1)('mysql')
    } else {
      const dashboardPath2 = path.join(global.applicationPath, 'src/log.js')
      Log = require(dashboardPath2)('mysql')
    }
    const connection2 = mysql.createConnection({ uri: databaseURL, multipleStatements: true })
    return connection2.query(setupSQLFile, (error) => {
      if (error) {
        Log.error('error setting up', error)
        return callback(new Error('unknown-error'))
      }
      connection2.destroy()
      const pool = mysql.createPool(databaseURL)
      const container = {
        exists: util.promisify((file, callback) => {
          if (!file) {
            return callback(new Error('invalid-file'))
          }
          const sql = mysql.format('SELECT EXISTS(SELECT 1 FROM objects WHERE path=?) AS item', [file])
          return pool.query(sql, (error, result) => {
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
        read: util.promisify((file, callback) => {
          if (!file) {
            return callback(new Error('invalid-file'))
          }
          return pool.query(mysql.format('SELECT * FROM objects WHERE path=?', [file]), (error, result) => {
            if (error) {
              Log.error('error reading', error)
              return callback(new Error('unknown-error'))
            }
            let data
            if (result && result.length && result[0].contents && result[0].contents) {
              data = result[0].contents.toString()
            }
            return callback(null, data)
          })
        }),
        readMany: util.promisify((path, files, callback) => {
          if (!files || !files.length) {
            return callback(new Error('invalid-files'))
          }
          const paths = []
          for (const file of files) {
            paths.push(mysql.escape(`${path}/${file}`))
          }
          return pool.query('SELECT * FROM objects WHERE path IN (' + paths.join(',') + ')', (error, result) => {
            if (error) {
              Log.error('error reading many', error)
              return callback(new Error('unknown-error'))
            }
            const data = {}
            if (result && result.length) {
              for (const row of result) {
                for (const file of files) {
                  if (row.path === `${path}/${file}`) {
                    data[file] = row.contents.toString()
                    break
                  }
                }
              }
            }
            return callback(null, data)
          })
        }),
        readBinary: util.promisify((file, callback) => {
          if (!file) {
            return callback(new Error('invalid-file'))
          }
          return pool.query(mysql.format('SELECT * FROM objects WHERE path=?', [file]), (error, result) => {
            if (error) {
              Log.error('error reading binary', error)
              return callback(new Error('unknown-error'))
            }
            return callback(null, result ? result[0] : null)
          })
        }),
        write: util.promisify((file, contents, callback) => {
          if (!file) {
            return callback(new Error('invalid-file'))
          }
          if (!contents && contents !== '') {
            return callback(new Error('invalid-contents'))
          }
          if (typeof (contents) !== 'number' && typeof (contents) !== 'string') {
            contents = JSON.stringify(contents)
          }
          return pool.query(mysql.format('INSERT INTO objects(path, contents) VALUES(?, ?) ON DUPLICATE KEY UPDATE contents=VALUES(`contents`)', [file, contents]), (error, result) => {
            if (error) {
              Log.error('error writing', error)
              return callback(new Error('unknown-error'))
            }
            if (!result || (result.affectedRows !== 1 && result.affectedRows !== 2)) {
              return callback(new Error('unknown-error'))
            }
            return callback()
          })
        }),
        writeBinary: util.promisify((file, buffer, callback) => {
          if (!file) {
            return callback(new Error('invalid-file'))
          }
          if (!buffer || !buffer.length) {
            return callback(new Error('invalid-buffer'))
          }
          return pool.query(mysql.format('INSERT INTO objects(path, contents) VALUES(?, ?) ON DUPLICATE KEY UPDATE contents=VALUES(`contents`)', [file, buffer]), (error, result) => {
            if (error) {
              Log.error('error writing binary', error)
              return callback(new Error('unknown-error'))
            }
            if (!result || result.affectedRows !== 1) {
              return callback(new Error('unknown-error'))
            }
            return callback(null, result ? result.count === 1 : null)
          })
        }),
        delete: util.promisify((file, callback) => {
          if (!file) {
            return callback(new Error('invalid-file'))
          }
          return pool.query(mysql.format('DELETE FROM objects WHERE path=?', [file]), (error, result) => {
            if (error) {
              Log.error('error deleting', error)
              return callback(new Error('unknown-error'))
            }
            return callback()
          })
        })
      }
      if (process.env.NODE_ENV === 'testing') {
        container.flush = util.promisify((callback) => {
          const connection2 = mysql.createConnection({ uri: databaseURL, multipleStatements: true })
          return connection2.query('DELETE FROM objects; DELETE FROM lists;', (error) => {
            if (error) {
              Log.error('error flushing', error)
              return callback(new Error('unknown-error'))
            }
            connection2.destroy()
            return callback()
          })
        })
      }
      return callback(null, container)
    })
  })
}
