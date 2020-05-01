/**
 * DynamoDB Streams Processor: A simple tool for working with DynamoDB Streams
 * @author Jeremy Daly <jeremy@jeremydaly.com>
 * @license MIT
 */

// Require the DynamoDB library and Converter
require('aws-sdk/clients/dynamodb')
const converter = require('aws-sdk/lib/dynamodb/converter')

// Require Set for instanceof comparison
const Set = require('aws-sdk/lib/dynamodb/set')

// Require deep-object-diff library for comparing records
const {
  diff,
  addedDiff,
  deletedDiff,
  detailedDiff,
  updatedDiff
} = require('deep-object-diff')

// Return function instead of taking the parameters and applying them here
const diffFunction = diffType => {
  switch (diffType) {
    case 'added':
      return addedDiff
    case 'deleted':
      return deletedDiff
    case 'updated':
      return updatedDiff
    case 'detailed':
      return detailedDiff
    default:
      return diff
  }
}

// unmarshalls the object and converts sets into arrays
const unmarshallWith = options => obj => {
  if (!obj) return undefined

  // unmarshall the object
  const item = converter.unmarshall(obj, options)
  // check each top level key for sets and convert appropriately
  return options.wrapSets
    ? item
    : Object.entries(item).reduce(
      (acc, [k, v]) => ({
        ...acc,
        [k]: v instanceof Set ? v.values : v
      }),
      {}
    )
}

const processWith = (diffType, options) => record => {
  if (!record.dynamodb) {
    throw new Error('Record is missing dynamodb property')
  }

  // Pre-apply unmarshal-function with options so we don't repeat ourselves
  const unmarshall = unmarshallWith(options)

  const Keys = unmarshall(record.dynamodb.Keys)
  const NewImage = unmarshall(record.dynamodb.NewImage)
  const OldImage = unmarshall(record.dynamodb.OldImage)
  const Diff =
    diffType &&
    OldImage &&
    NewImage &&
    diffFunction(diffType)(OldImage, NewImage)

  return {
    ...record,
    dynamodb: {
      ...(Keys && { Keys }),
      ...(NewImage && { NewImage }),
      ...(OldImage && { OldImage }),
      ...(Diff && { Diff })
    }
  }
}

// Export main function
module.exports = (records, diffType = false, options = {}) => {
  const process = processWith(diffType, options)
  switch (true) {
    case Array.isArray(records):
      return records.map(process)
    case records instanceof Object:
      return process(records)
    default:
      throw new Error('Input must be array or object')
  }
}
