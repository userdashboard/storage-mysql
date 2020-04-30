const connectionString = process.env.DATABASE_URL || 'mysql://localhost:3306/testing'
const mysql = require('mysql2')
const pool = mysql.createPool(connectionString)
const util = require('util')

module.exports = {
  add: util.promisify(add),
  count: util.promisify(count),
  exists: util.promisify(exists),
  list: util.promisify(list),
  listAll: util.promisify(listAll),
  remove: util.promisify(remove)
}

function exists (path, objectid, callback) {
  return pool.query(mysql.format('SELECT EXISTS(SELECT 1 FROM lists WHERE path=? AND objectid=?) AS item', [path, objectid]), (error, result) => {
    if (error) {
      return callback(error)
    }
    if (!result || !result.length || !result[0] || result[0].item === undefined) {
      return callback(new Error('unknown-error'))
    }
    return callback(null, result[0].item === 1)
  })
}

function add (path, objectid, callback) {
  return exists(path, objectid, (error, existing) => {
    if (error) {
      return callback(error)
    }
    if (existing) {
      return callback()
    }
    if (objectid === true || objectid === false) {
      objectid = objectid.toString()
    }
    return pool.query(mysql.format('INSERT INTO lists(path, objectid) VALUES (?, ?)', [path, objectid]), (error, result) => {
      if (error) {
        return callback(error)
      }
      if (!result || !result.insertId) {
        throw new Error('unknown-error')
      }
      return callback(null, result)
    })
  })
}

function count (path, callback) {
  return pool.query(mysql.format('SELECT COUNT(*) AS counter FROM lists WHERE path=?', [path]), (error, result) => {
    if (error) {
      return callback(error)
    }
    if (!result || !result.length || !result[0] || result[0].counter === undefined) {
      return callback(new Error('unknown-error'))
    }
    return callback(null, result[0].counter)
  })
}

function listAll (path, callback) {
  return pool.query(mysql.format('SELECT objectid FROM lists WHERE path=? ORDER BY id DESC', [path]), (error, result) => {
    if (error) {
      return callback(error)
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
}

function list (path, offset, pageSize, callback) {
  offset = offset || 0
  if (pageSize === null || pageSize === undefined) {
    pageSize = global.pageSize
  }
  if (offset < 0) {
    return callback(new Error('invalid-offset'))
  }
  return pool.query(mysql.format(`SELECT objectid FROM lists WHERE path=? ORDER BY id DESC LIMIT ${pageSize} OFFSET ${offset}`, [path]), (error, result) => {
    if (error) {
      return callback(error)
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
}

function remove (_, objectid, callback) {
  objectid = objectid.toString()
  return pool.query(mysql.format('DELETE FROM lists WHERE objectid=?', [objectid]), (error, result) => {
    if (error) {
      return callback(error)
    }
    if (result.affectedRows === 0) {
      return callback(new Error('unknown-error'))
    }
    return callback()
  })
}
