import * as csv from 'csv'

const data = await Bun.file("../name_basics.csv").text()
const parser = csv.parse({
//   delimiter: '\t',
  columns: true,
//   skip_empty_lines: true
})

export const records: any[] = await new Promise((resolve, reject) => {
  const results: any[] = []
  parser.on('readable', () => {
    let record
    while ((record = parser.read()) !== null) {
      results.push(record)
    }
  })
  parser.on('error', reject)
  parser.on('end', () => resolve(results))
  parser.write(data)
  parser.end()
})

