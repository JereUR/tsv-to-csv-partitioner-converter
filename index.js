const fs = require('fs')
const Papa = require('papaparse')

const filePath = './data-file/data.tsv'
const chunkSize = 90 * 1024 * 1024

let readStream = fs.createReadStream(filePath, {
  encoding: 'utf8',
  highWaterMark: chunkSize
})
let chunkCount = 0

readStream.on('data', (chunk) => {
  chunkCount++
  const chunkFilePath = `./results-chunks/chunk_${chunkCount}.tsv`

  fs.writeFile(chunkFilePath, chunk, 'utf8', (err) => {
    if (err) {
      console.error(`Error al escribir el archivo ${chunkFilePath}`, err)
    } else {
      console.log(`Archivo ${chunkFilePath} creado exitosamente.`)

      Papa.parse(chunk, {
        delimiter: '\t',
        complete: (result) => {
          const csvContent = Papa.unparse(result.data)
          const csvFilePath = `./results-csv/chunk_${chunkCount}.csv`
          fs.writeFileSync(csvFilePath, csvContent)
          console.log(`Archivo ${csvFilePath} convertido exitosamente.`)
        },
        error: (err) => {
          console.error(`Error al convertir ${chunkFilePath} a CSV:`, err)
        }
      })
    }
  })
})

readStream.on('end', () => {
  console.log('Lectura del archivo completa.')
})
