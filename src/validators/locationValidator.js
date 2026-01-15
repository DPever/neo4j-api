import { ValidationError, isFiniteNumber } from './common.js';

export function validateDirection(direction) {
  const allowed = new Set(['R', 'D', 'B']);
  if (!allowed.has(direction)) {
    throw new ValidationError(`Invalid direction '${direction}'. Allowed: R, D, B`);
  }
}

export function validatePosition(position) {
  if (position == null) return;

  const { latitude, longitude } = position;

  if (!isFiniteNumber(latitude) || !isFiniteNumber(longitude)) {
    throw new ValidationError('position.latitude and position.longitude must be numbers');
  }
// These are valid latitude/longitude checks for the entire world, but we limit our checks to North America
//  if (latitude < -90 || latitude > 90) {
//    throw new ValidationError(`position.latitude out of range (-90..90): ${latitude}`);
//  }
//  if (longitude < -180 || longitude > 180) {
//    throw new ValidationError(`position.longitude out of range (-180..180): ${longitude}`);
//  }
  if (latitude < 20 || latitude > 80) {
    throw new ValidationError(`position.latitude out of range for North America (20..80): ${latitude}`);
  }
  if (longitude < -130 || longitude > -60) {
    throw new ValidationError(`position.longitude out of range for North America(-130..-60): ${longitude}`);
  }
}

export function validateZone(zone, validZones, pipelineCode) {
  if (!validZones.has(zone)) {
    throw new ValidationError(
      `Invalid zone '${zone}' for pipeline ${pipelineCode}`,
      { allowedZonesSample: Array.from(validZones).slice(0, 20) }
    );
  }
}

