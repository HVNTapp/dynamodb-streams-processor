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
  updatedDiff,
} = require('deep-object-diff')

// Export main function
module.exports = (records, diffType = false, options = {}) => {
  if (Array.isArray(records)) {
    return records.map((record) => process(record, diffType, options))
  } else if (records instanceof Object) {
    return process(records, diffType, options)
  } else {
    throw new Error('Input must be array or object')
  }
}

const process = (rec, diffType, options) => {
  if (!rec.dynamodb) {
    throw new Error('Record is missing dynamodb property')
  }

  const Keys = rec.dynamodb.Keys
    ? unmarshall(rec.dynamodb.Keys, options)
    : null
  const NewImage = rec.dynamodb.NewImage
    ? unmarshall(rec.dynamodb.NewImage, options)
    : null
  const OldImage = rec.dynamodb.OldImage
    ? unmarshall(rec.dynamodb.OldImage, options)
    : null
  const Diff =
    diffType &&
    rec.dynamodb.NewImage &&
    rec.dynamodb.OldImage &&
    compare(OldImage, NewImage, diffType)

  return {
    ...rec,
    dynamodb: {
      ...(Keys && { Keys }),
      ...(NewImage && { NewImage }),
      ...(OldImage && { OldImage }),
      ...(Diff && { Diff }),
    },
  }
}

// unmarshalls the object and converts sets into arrays
const unmarshall = (obj, options) => {
  // unmarshall the object
  const item = converter.unmarshall(obj, options)
  // check each top level key for sets and convert appropriately
  return options.wrapSets
    ? item
    : Object.entries(item).reduce(
      (acc, [k, v]) => ({
        ...acc,
        [k]: v instanceof Set ? v.values : v,
      }),
      {}
    )
}

// Use deep-object-diff to compare records
const compare = (a, b, diffType) =>
  diffType === 'added'
    ? addedDiff(a, b)
    : diffType === 'deleted'
      ? deletedDiff(a, b)
      : diffType === 'updated'
        ? updatedDiff(a, b)
        : diffType === 'detailed'
          ? detailedDiff(a, b)
          : diff(a, b)
