export class ValidationError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = 'ValidationError';
    this.status = 400;
    this.details = details;
  }
}

export const isFiniteNumber = (v) =>
  typeof v === 'number' && Number.isFinite(v);
