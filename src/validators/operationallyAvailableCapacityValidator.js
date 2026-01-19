import { ValidationError } from './common.js';

const enumSet = (values) => ({
  values,
  set: new Set(values),
  message: () => values.join(', ')
});

const DIR = enumSet(['', 'Delivery', 'Receipt', 'Bidirectional', 'Throughput']);
const GROSS_NET = enumSet(['GROSS', 'NET']);
const IT = enumSet(['Y', 'N', '']);
const SCHED_STATUS = enumSet(['PRELIM', 'FINAL']);

export function validateOacRow(oac, idx = 0) {
  const errs = [];

  const add = (msg) => errs.push(`Row ${idx}: ${msg}`);

  // suspend enforcement of direction for now
  // if (oac.direction && !DIR.set.has(oac.direction)) add(`Invalid direction '${oac.direction}'. Allowed: ${DIR.message()}`);
  if (oac.grossOrNet && !GROSS_NET.set.has(oac.grossOrNet)) add(`Invalid grossOrNet '${oac.grossOrNet}'. Allowed: ${GROSS_NET.message()}`);
  if (oac.itIndicator && !IT.set.has(oac.itIndicator)) add(`Invalid itIndicator '${oac.itIndicator}'. Allowed: ${IT.message()}`);
  if (oac.schedStatus && !SCHED_STATUS.set.has(oac.schedStatus)) add(`Invalid schedStatus '${oac.schedStatus}'. Allowed: ${SCHED_STATUS.message()}`);

  // numeric sanity checks (no negatives, etc.)
  const nonNegInts = ['designCapacity','operatingCapacity','operationallyAvailableCapacity','totalSchedQty'];
  for (const f of nonNegInts) {
    const v = oac[f];
    if (!Number.isFinite(v)) add(`${f} must be a number`);
    else if (v < 0) add(`${f} cannot be negative`);
  }

  // basic date/time format checks (lightweight)
  if (!/^\d{4}-\d{2}-\d{2}$/.test(oac.flowDate)) add(`flowDate must be YYYY-MM-DD`);
  // postingDate should be ISO-ish; keep this loose
  if (typeof oac.postingDate !== 'string' || oac.postingDate.length < 10) add(`postingDate must be an ISO datetime string`);

  if (errs.length) {
    throw new ValidationError('OAC validation failed', { errors: errs });
  }
}

/**
 * Validate a batch: collects all row errors and throws one ValidationError
 * containing an array of row errors.
 */
export function validateOacBatch(rows) {
  const errors = [];

  rows.forEach((row, idx) => {
    try {
      validateOacRow(row, idx);
    } catch (e) {
      const list = e?.details?.errors ?? [`Row ${idx}: ${e.message}`];
      errors.push(...list);
    }
  });

  if (errors.length) {
    throw new ValidationError('OAC batch validation failed', { errors });
  }
}
