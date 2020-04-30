const connectionString = process.env.DATABASE_URL || 'mysql://localhost:3306/testing'
const mysql = require('mysql2/promise')
const connection = mysql.createConnection(connectionString)
connection.connect((error) => {
  if (error) {
    if (process.env.DEBUG_ERRORS) {
      console.error('[storage-mysql]', error)
    }
    throw error
  }
})

module.exports = {
  exists,
  read,
  readMany,
  readImage,
  write,
  writeImage,
  deleteFile
}

if (process.env.NODE_ENV === 'testing') {
  module.exports.flush = async () => {
    await connection.query('DROP TABLE IF EXISTS objects')
    await connection.query('DROP TABLE IF EXISTS lists')
    const fs = require('fs')
    const path = require('path')
    let setupSQLFile = path.join(__dirname, 'setup.sql')
    if (!fs.existsSync(setupSQLFile)) {
      setupSQLFile = path.join(global.applicationPath, 'node_modules/@userdashboard/storage-mysql/setup.sql')
    }
    setupSQLFile = fs.readFileSync(setupSQLFile).toString()
    await connection.query(setupSQLFile)
  }
}

async function exists (file) {
  if (!file) {
    throw new Error('invalid-file')
  }
  const result = await connection.query('SELECT EXISTS(SELECT 1 FROM objects WHERE fullpath=$1)', [file])
  return result && result.rows && result.rows.length ? result.rows[0].exists : null
}

async function deleteFile (file) {
  if (!file) {
    throw new Error('invalid-file')
  }
  const result = await connection.query('DELETE FROM objects WHERE fullpath=$1', [file])
  return result ? result.count === 1 : null
}

async function write (file, contents) {
  if (!file) {
    throw new Error('invalid-file')
  }
  if (!contents && contents !== '') {
    throw new Error('invalid-contents')
  }
  if (typeof (contents) !== 'number' && typeof (contents) !== 'string') {
    contents = JSON.stringify(contents)
  }
  contents = Buffer.isBuffer(contents) ? contents : Buffer.from(contents)
  contents = `\\x${contents.toString('hex')}`
  await connection.query('INSERT INTO objects(fullpath, blob) VALUES($1, $2) ON CONFLICT(fullpath) DO UPDATE SET blob=$2', [file, contents])
}

async function writeImage (file, buffer) {
  if (!file) {
    throw new Error('invalid-file')
  }
  if (!buffer || !buffer.length) {
    throw new Error('invalid-buffer')
  }
  const result = await connection.query('INSERT INTO objects(fullpath, blob) VALUES($1, $2) ON CONFLICT(fullpath) DO UPDATE SET blob=$2', [file, buffer])
  return result ? result.count === 1 : null
}

async function read (file) {
  if (!file) {
    throw new Error('invalid-file')
  }
  const result = await connection.query('SELECT * FROM objects WHERE fullpath=$1', [file])
  let data
  if (result && result.rows && result.rows.length && result.rows[0].blob) {
    data = result.rows[0].blob.toString()
  }
  return data
}

async function readMany (path, files) {
  if (!files || !files.length) {
    throw new Error('invalid-files')
  }
  const fullPaths = []
  for (const file of files) {
    fullPaths.push(`${path}/${file}`)
  }
  const result = await connection.query('SELECT * FROM objects WHERE fullpath=ANY($1)', [fullPaths])
  const data = {}
  if (result && result.rows && result.rows.length) {
    for (const row of result.rows) {
      for (const file of files) {
        if (row.fullpath === `${path}/${file}`) {
          data[file] = row.blob.toString()
          break
        }
      }
    }
  }
  return data
}

async function readImage (file) {
  if (!file) {
    throw new Error('invalid-file')
  }
  if (!file) {
    throw new Error('invalid-file')
  }
  const result = await connection.query('SELECT * FROM objects WHERE fullpath=$1', [file])
  return result ? result.rows[0] : null
}
