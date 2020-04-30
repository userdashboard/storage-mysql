const connectionString = process.env.DATABASE_URL || 'mysql://localhost:3306/testing'
const mysql = require('mysql2')
const pool = mysql.createPool(connectionString)
const util = require('util')

module.exports = {
  exists: util.promisify(exists),
  read: util.promisify(read),
  readMany: util.promisify(readMany),
  readImage: util.promisify(readImage),
  write: util.promisify(write),
  writeImage: util.promisify(writeImage),
  deleteFile: util.promisify(deleteFile)
}

if (process.env.NODE_ENV === 'testing') {
  const flushAll = util.promisify((callback) => {
    var connection2 = mysql.createConnection({ uri: connectionString, multipleStatements: true })
    return connection2.query('DROP TABLE IF EXISTS objects; DROP TABLE IF EXISTS lists;', () => {
      const fs = require('fs')
      const path = require('path')
      let setupSQLFile = path.join(__dirname, 'setup.sql')
      if (!fs.existsSync(setupSQLFile)) {
        setupSQLFile = path.join(global.applicationPath, 'node_modules/@userdashboard/storage-mysql/setup.sql')
      }
      setupSQLFile = fs.readFileSync(setupSQLFile).toString()
      return connection2.query(setupSQLFile, (error) => {
        if (error) {
          return callback(error)
        }
        connection2.destroy()
        return callback()
      })
    })
  })
  module.exports.flush = async () => {
    await flushAll()
  }
}

function exists (file, callback) {
  if (!file) {
    return callback(new Error('invalid-file'))
  }
  const sql = mysql.format('SELECT EXISTS(SELECT 1 FROM objects WHERE path=?) AS item', [file])
  return pool.query(sql, (error, result) => {
    if (error) {
      return callback(error)
    }
    if (!result || !result.length || !result[0] || result[0].item === undefined) {
      return callback(new Error('unknown-error'))
    }
    return callback(null, result[0].item === 1)
  })
}

function deleteFile (file, callback) {
  if (!file) {
    return callback(new Error('invalid-file'))
  }
  return pool.query(mysql.format('DELETE FROM objects WHERE path=?', [file]), (error, result) => {
    if (error) {
      return callback(error)
    }
    if (!result || result.affectedRows !== 1) {
      return callback(new Error('unknown-error'))
    }
    return callback()
  })
}

function write (file, contents, callback) {
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
      return callback(error)
    }
    if (!result || (result.affectedRows !== 1 && result.affectedRows !== 2)) {
      return callback(new Error('unknown-error'))
    }
    return callback()
  })
}

function writeImage (file, buffer, callback) {
  if (!file) {
    return callback(new Error('invalid-file'))
  }
  if (!buffer || !buffer.length) {
    return callback(new Error('invalid-buffer'))
  }
  return pool.query(mysql.format('INSERT INTO objects(path, contents) VALUES(?, ?) ON DUPLICATE KEY UPDATE contents=VALUES(`contents`)', [file, buffer]), (error, result) => {
    if (error) {
      return callback(error)
    }
    if (!result || result.affectedRows !== 1) {
      return callback(new Error('unknown-error'))
    }
    return callback(null, result ? result.count === 1 : null)
  })
}

function read (file, callback) {
  if (!file) {
    return callback(new Error('invalid-file'))
  }
  return pool.query(mysql.format('SELECT * FROM objects WHERE path=?', [file]), (error, result) => {
    if (error) {
      return callback(error)
    }
    let data
    if (result && result.length && result[0].contents && result[0].contents) {
      data = result[0].contents.toString()
    }
    return callback(null, data)
  })
}

function readMany (path, files, callback) {
  if (!files || !files.length) {
    return callback(new Error('invalid-files'))
  }
  const paths = []
  for (const file of files) {
    paths.push(mysql.escape(`${path}/${file}`))
  }
  return pool.query('SELECT * FROM objects WHERE path IN (' + paths.join(',') + ')', (error, result) => {
    if (error) {
      return callback(error)
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
}

function readImage (file, callback) {
  if (!file) {
    return callback(new Error('invalid-file'))
  }
  return pool.query(mysql.format('SELECT * FROM objects WHERE path=?', [file]), (error, result) => {
    if (error) {
      return callback(error)
    }
    return callback(null, result ? result[0] : null)
  })
}
