import 'dotenv/config';
import express from 'express';
import neo4j from 'neo4j-driver';
import swaggerUi from 'swagger-ui-express';
import swaggerJsdoc from 'swagger-jsdoc';

// ---- Config ----
const {
  NEO4J_URI,
  NEO4J_USERNAME,
  NEO4J_PASSWORD,
  PORT = 8080,
  ENABLE_WRITES = 'false',
} = process.env;

if (!NEO4J_URI || !NEO4J_USERNAME || !NEO4J_PASSWORD) {
  console.error('Missing required Neo4j environment variables. Check .env.');
  process.exit(1);
}

// ---- Neo4j Driver ----
const driver = neo4j.driver(NEO4J_URI, neo4j.auth.basic(NEO4J_USERNAME, NEO4J_PASSWORD));
const app = express();
app.use(express.json({ limit: '1mb' }));

// ---- Swagger/OpenAPI ----
const swaggerOptions = {
  definition: {
    openapi: '3.0.3',
    info: {
      title: 'Gas Scheduling Optimization API',
      version: '1.0.0',
      description: 'Endpoints for locations, nominations, and constraints'
    },
    servers: [
      { url: process.env.BASE_URL || `http://localhost:${PORT}` }
      // add { url: 'https://<your-render-app>.onrender.com' } if you want a fixed server shown
    ],
    components: {
      securitySchemes: {
        // remove this if you’re not using an API key guard
        ApiKeyHeader: { type: 'apiKey', in: 'header', name: 'x-api-key' }
      },
      schemas: {
        Constraint: {
          type: 'object',
          properties: {
            id: { type: 'integer' },
            reason: { type: 'string' },
            kind: { type: 'string' },
            start: { type: 'string', format: 'date-time' },
            end:   { type: 'string', format: 'date-time', nullable: true },
            percent: { type: 'number' }
          }
        },
        Nomination: {
          type: 'object',
          properties: {
            nomId: { type: 'integer' },
            pipeline: { type: 'string' },
            TA: { type: 'string' },
            flowDate: { type: 'string', format: 'date' },
            cycle: { type: 'string' },
            receiptLocation: { type: 'string' },
            receiptVolume: { type: 'number' },
            fuelLoss: { type: 'number' },
            deliveryLocation: { type: 'string' },
            deliveryVolume: { type: 'number' },
            impactedLocations: {
              type: 'array',
              items: {
                type: 'object',
                properties: {
                  locationName: { type: 'string' },
                  locationPipeline: { type: 'string' },
                  constraintStart: { type: 'string', format: 'date-time' },
                  constraintEnd:   { type: 'string', format: 'date-time', nullable: true },
                  constraintPercent: { type: 'number' }
                }
              }
            }
          }
        },
        OperationalFlow: {
          type: 'object',
          properties: {
            pipeline: { type: 'string' },
            locationNumber: { type: 'integer' },
            flowDate: { type: 'string', format: 'date-time' },
            cycle: { type: 'string' },
            operationalCapacity: { type: 'number' },
            scheduledVolume: { type: 'number' },
            utilizationPercent: { type: 'number' }
          }
        }
      }
    }
    // , security: [{ ApiKeyHeader: [] }] // uncomment if you enforce x-api-key globally
  },
  apis: [] // we’re building the spec entirely here (no JSDoc scanning yet)
};

const swaggerSpec = swaggerJsdoc(swaggerOptions);
swaggerSpec.paths = {
  ...(swaggerSpec.paths || {}),

  '/health': {
    get: {
      summary: 'Health check',
      tags: ['System'],
      responses: {
        200: {
          description: 'Service healthy',
          content: { 'application/json': { schema: { type: 'object', properties: { status: { type: 'string' } } } } }
        },
        500: {
          description: 'Service error',
          content: { 'application/json': { schema: { type: 'object', properties: { status: { type: 'string' }, error: { type: 'string' } } } } }
        }
      }
    }
  },

  '/pipelines': {
    get: {
      summary: 'List of pipelines',
      tags: ['Reference Data'],
      responses: {
        200: {
          description: 'Found',
          content: {
            'application/json': {
              schema: {
                type: 'object',
                properties: {
                  count: { type: 'integer' },
                  pipelines: {
                    type: 'array',
                    items: {
                      type: 'object',
                      properties: {
                        pipeline: { type: 'object' }
                      }
                    }
                  }
                }
              }
            }
          }
        },
        404: { description: 'Not found' }
      }
    }
  },

  '/locations/{pipeline}': {
    get: {
      summary: 'Locations for a pipeline',
      tags: ['Reference Data'],
      parameters: [
        { name: 'pipeline', in: 'path', required: true, schema: { type: 'string' }, 
          description: 'Filter by pipeline name (e.g., ANR, TETCO)'
        },
        { name: 'limit', in: 'query', required: false, schema: { type: 'integer', default: 100 }, 
          description: 'maximum number of locations to return', example: '100'
        },
        { name: 'skip', in: 'query', required: false, schema: { type: 'integer', default: 0 }, 
          description: 'number of locations to skip for pagination', example: '0'
        }
      ],
      responses: {
        200: {
          description: 'Found',
          content: {
            'application/json': {
              schema: {
                type: 'object',
                properties: {
                  count: { type: 'integer' },
                  locations: {
                    type: 'array',
                    items: {
                      type: 'object',
                      properties: {
                        location: { type: 'object' }
                      }
                    }
                  }
                }
              }
            }
          }
        },
        404: { description: 'Not found' }
      }
    }
  },

  '/pipeline-segments/{pipeline}': {
    get: {
      summary: 'List of pipeline segments for a pipeline',
      tags: ['Reference Data'],
      parameters: [
        { name: 'pipeline', in: 'path', required: true, schema: { type: 'string' }, 
          description: 'Filter by pipeline name (e.g., ANR, TETCO)'
        },
        { name: 'limit', in: 'query', required: false, schema: { type: 'integer', default: 100 }, 
          description: 'maximum number of segments to return', example: '100'
        },
        { name: 'skip', in: 'query', required: false, schema: { type: 'integer', default: 0 }, 
          description: 'number of segments to skip for pagination', example: '0'
        }
      ],
      responses: {
        200: {
          description: 'Found',
          content: {
            'application/json': {
              schema: {
                type: 'object',
                properties: {
                  count: { type: 'integer' },
                  segments: {
                    type: 'array',
                    items: {
                      type: 'object',
                      properties: {
                        segment: { type: 'object' }
                      }
                    }
                  }
                }
              }
            }
          }
        },
        404: { description: 'Not found' }
      }
    }
  },

  '/noms/{pipeline}/{flowDate}': {
    get: {
      summary: 'All nominations on a pipeline for a gas day',
      tags: ['Nominations'],
      parameters: [
        { name: 'pipeline', in: 'path', required: true, schema: { type: 'string' }, 
          description: 'Filter by pipeline name (e.g., ANR, TETCO)'
        },
        { name: 'flowDate', in: 'path', required: true,
          schema: { type: 'string', format: 'date', pattern: '^\\d{4}-\\d{2}-\\d{2}$' },
          example: '2025-11-01'
        }
      ],
      responses: {
        200: {
          description: 'List of nominations',
          content: { 'application/json': { schema: {
            type: 'object',
            properties: {
              flowDate: { type: 'string', format: 'date' },
              count: { type: 'integer' },
              nominations: { type: 'array', items: { $ref: '#/components/schemas/Nomination' } }
            }
          } } }
        },
        404: { description: 'Not found' }
      }
    }
  },

  '/volumes/historical-flow/{pipeline}/{startDate}/{endDate}': {
    get: {
      summary: 'Historic flow volumes and operational capacity at a location for a pipeline and date range',
      tags: ['Volume'],
      parameters: [
        { name: 'pipeline', in: 'path', required: true, schema: { type: 'string' }, example: 'ANR' },
        { name: 'startDate',  in: 'path', required: true,
          schema: { type: 'string', format: 'date', pattern: '^\\d{4}-\\d{2}-\\d{2}$' },
          example: '2025-11-01'
        },
        { name: 'endDate',  in: 'path', required: true,
          schema: { type: 'string', format: 'date', pattern: '^\\d{4}-\\d{2}-\\d{2}$' },
          example: '2025-11-30'
        },
        { name: 'locationNumber', in: 'query', required: false, schema: { type: 'integer' } },
        { name: 'cycle', in: 'query', required: false, schema: { type: 'string' }, description: 'Filter by cycle (e.g., TIM, EVN, ID1, ID2, ID3)' },
        { name: 'limit', in: 'query', required: false, schema: { type: 'integer', default: 100 }, 
          description: 'maximum number of flow rows to return', example: '100'
        },
        { name: 'skip', in: 'query', required: false, schema: { type: 'integer', default: 0 }, 
          description: 'number of flow rows to skip for pagination', example: '0'
        }
      ],
      responses: {
        200: {
          description: 'Operational flows',
          content: { 'application/json': { schema: {
            type: 'object',
            properties: {
              params: { type: 'object' },
              count: { type: 'integer' },
              flows: { type: 'array', items: { $ref: '#/components/schemas/OperationalFlow' } },
              page: { type: 'object' }
            }
          } } }
        }
      }
    }
  },

  '/notices/constrained-noms/{locationName}/{beforeDate}': {
    get: {
      summary: 'Nominations that pass through a location and had prior constraints before a date',
      tags: ['Notices'],
      parameters: [
        { name: 'locationName', in: 'path', required: true, schema: { type: 'string' }, example: 'SPARTA-MUSKEGON' },
        { name: 'beforeDate',  in: 'path', required: true,
          schema: { type: 'string', format: 'date', pattern: '^\\d{4}-\\d{2}-\\d{2}$' },
          example: '2025-11-01'
        },
        { name: 'pipeline', in: 'query', required: false, schema: { type: 'string' }, example: 'ANR' }
      ],
      responses: {
        200: {
          description: 'Constrained nominations',
          content: { 'application/json': { schema: {
            type: 'object',
            properties: {
              beforeDate: { type: 'string', format: 'date' },
              count: { type: 'integer' },
              nominations: { type: 'array', items: { $ref: '#/components/schemas/Nomination' } }
            }
          } } }
        },
        404: { description: 'Not found' }
      }
    }
  }
};

// UI at /swagger, and raw JSON at /openapi.json
app.use('/swagger', swaggerUi.serve, swaggerUi.setup(swaggerSpec, { explorer: true }));
app.get('/openapi.json', (_req, res) => res.json(swaggerSpec));


// ---- Helper Functions ----
// Helper to standardize sessions/transactions
async function runQuery(cypher, params = {}, mode = 'READ') {
  const session = driver.session({ defaultAccessMode: mode === 'WRITE' ? neo4j.session.WRITE : neo4j.session.READ });
  try {
    return await session.run(cypher, params);
  } finally {
    await session.close();
  }
}

// Convert Neo4j types to plain JS types recursively
function toPlain(value) {
  if (value == null) return value;
  // Neo4j integers
  if (neo4j.isInt?.(value)) return value.inSafeRange() ? value.toNumber() : value.toString();

  // Arrays
  if (Array.isArray(value)) return value.map(toPlain);

  // Temporal types or plain objects
  if (typeof value === 'object') {
    // Neo4j temporal values stringify nicely to ISO
    if (typeof value.toString === 'function' && (
        Object.prototype.hasOwnProperty.call(value, 'year') ||
        Object.prototype.hasOwnProperty.call(value, 'month') ||
        Object.prototype.hasOwnProperty.call(value, 'day'))) {
      return value.toString();
    }
    const out = {};
    for (const [k, v] of Object.entries(value)) out[k] = toPlain(v);
    return out;
  }
  return value;
}

function parseBool(v, fallback=false) {
  if (typeof v === 'boolean') return v;
  if (typeof v !== 'string') return fallback;
  const t = v.toLowerCase().trim();
  return t === '1' || t === 'true' || t === 'yes';
}

const writesEnabled = parseBool(ENABLE_WRITES);

// ---- Routes ----

app.get('/health', async (req, res) => {
  try {
    await driver.getServerInfo(); // lightweight handshake
    res.json({ status: 'ok' });
  } catch (e) {
    res.status(500).json({ status: 'error', error: e.message });
  }
});

// GET /pipelines  — fetch all pipelines
app.get('/pipelines', async (req, res) => {
  try {
    const result = await runQuery(
      `
      MATCH (n:Pipeline) RETURN n.code, n.name, n.operator
      ORDER BY n.name
      `
    );

    // Map records to plain JS objects
    const pipelines = result.records.map(r => {
      const obj = {};
      for (const key of r.keys) {
        obj[key] = toPlain(r.get(key));
      }
      return obj;
    });

    res.json({ count: pipelines.length, pipelines });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /locations/:pipeline?limit=100&skip=0  — fetch all Locations for a given pipeline w/ pagination
// Example: /locations/ANR?limit=50&skip=0
app.get('/locations/:pipeline', async (req, res) => {
  const pipeline = req.params.pipeline;
  const limit = parseInt(req.query.limit) || 100;
  const skip  = parseInt(req.query.skip)  || 0;

  // Basic input validation
  if (!pipeline || typeof pipeline !== 'string') {
    return res.status(400).json({ error: "pipeline is required" });
  }
  try {
    const result = await runQuery(
      `
      MATCH (l:Location)
      WHERE l.pipeline = $pipeline
      RETURN
        l.name        AS name,
        l.number      AS number,
        l.type        AS type,
        l.zone        AS zone,
        l.area        AS area,
        l.direction   AS direction,
        l.up_down_name AS upDownName,
        l.up_down_number AS upDownNumber,
        l.position AS position
      ORDER BY l.number SKIP toInteger($skip) LIMIT toInteger($limit)
      `,
      { pipeline, limit, skip }
    );

    // Map records to plain JS objects
    const locations = result.records.map(r => {
      const obj = {};
      for (const key of r.keys) {
        obj[key] = toPlain(r.get(key));
      }
      return obj;
    });

    res.json({ count: locations.length, pipeline, locations, page: { skip: Number(skip), limit: Number(limit) } });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /pipeline-segments/:pipeline?limit=100&skip=0  — fetch all segments for a given pipeline w/ pagination
// Example: /pipeline-segments/ANR?limit=50&skip=0
app.get('/pipeline-segments/:pipeline', async (req, res) => {
  const pipeline = req.params.pipeline;
  const limit = parseInt(req.query.limit) || 100;
  const skip  = parseInt(req.query.skip)  || 0;

  // Basic input validation
  if (!pipeline || typeof pipeline !== 'string') {
    return res.status(400).json({ error: "pipeline is required" });
  }
  try {
    const result = await runQuery(
      `
      MATCH (src:Location)-[r:Segment_Locations]->(dst:Location)
      WHERE r.pipeline = $pipeline
      RETURN
        id(r) AS segmentId, 
        src.name AS sourceName, src.number as sourceNumber,
        dst.name AS destName, dst.number as destNumber
      ORDER BY src.number, dst.number SKIP toInteger($skip) LIMIT toInteger($limit)
      `,
      { pipeline, limit, skip }
    );

    // Map records to plain JS objects
    const segments = result.records.map(r => {
      const obj = {};
      for (const key of r.keys) {
        obj[key] = toPlain(r.get(key));
      }
      return obj;
    });

    res.json({ pipeline, count: segments.length, segments, page: { skip: Number(skip), limit: Number(limit) } });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /noms/:pipeline/:flowDate  — fetch all nominations on a pipeline for a given flow date (YYYY-MM-DD)
// Example: /noms/ANR/2025-11-01
app.get('/noms/:pipeline/:flowDate(\\d{4}-\\d{2}-\\d{2})', async (req, res) => {
  const pipeline = req.params.pipeline;
  const flowDate = req.params.flowDate;

  try {
    const result = await runQuery(
      `
      WITH date($flowDate) AS d
      WITH d,
        datetime({date: d}) AS dayStart,
        datetime({date: d}) + duration('P1D') - duration('PT1S') AS dayEnd

      MATCH (rcpt:Location)-[n:NOMINATED]->(dlv:Location)
      WHERE n.pipeline = $pipeline AND n.flowDate = d

      CALL {
        WITH rcpt, dlv, dayStart, dayEnd
        MATCH p = allShortestPaths( (rcpt)-[:Segment_Locations*]->(dlv) )
        UNWIND nodes(p) AS loc
        OPTIONAL MATCH (loc)-[:HAS_CONSTRAINT]->(c:Constraint)
          WHERE c.start <= dayEnd AND c.end >= dayStart
        WITH collect(DISTINCT {loc: loc, c: c}) AS raw
        RETURN [x IN raw WHERE x.c IS NOT NULL] AS hits   // [] if none
      }

      RETURN
        n.nomId          AS nomId,
        n.pipeline       AS pipeline,
        n.flowDate       AS flowDate,
        n.cycle          AS cycle,
        rcpt.name        AS receiptLocation,
        n.receiptVolume  AS receiptVolume,
        n.fuelLoss       AS fuelLoss,
        dlv.name         AS deliveryLocation,
        n.deliveryVolume AS deliveryVolume,
        [h IN hits | {
          locationName: h.loc.name,
          locationPipeline: h.loc.pipeline,
          constraintStart: h.c.start,
          constraintEnd: h.c.end,
          constraintPercent: h.c.percent
        }] AS impactedLocations
      ORDER BY n.pipeline, n.nomId
      `,
      { pipeline, flowDate }
    );

    // Map records to plain JS objects
    const nominations = result.records.map(r => {
      const obj = {};
      for (const key of r.keys) {
        obj[key] = toPlain(r.get(key));
      }
      return obj;
    });

    res.json({ pipeline, flowDate, count: nominations.length, nominations });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /notices/constrained-noms/:flowDate  — fetch all constrained nominations for a given flow date (YYYY-MM-DD)
// Example: /notices/constrained-noms/2025-11-01
app.get('/notices/constrained-noms/:flowDate(\\d{4}-\\d{2}-\\d{2})', async (req, res) => {
  const flowDate = req.params.flowDate;

  try {
    const result = await runQuery(
      `
      WITH date($flowDate) AS d
      WITH d,
        datetime({date: d}) AS dayStart,
        datetime({date: d}) + duration('P1D') - duration('PT1S') AS dayEnd

      MATCH (rcpt:Location)-[n:NOMINATED]->(dlv:Location)
      WHERE n.flowDate = d

      CALL {
        WITH rcpt, dlv, dayStart, dayEnd
        MATCH p = allShortestPaths( (rcpt)-[:Segment_Locations*]->(dlv) )
        UNWIND nodes(p) AS loc
        MATCH (loc)-[:HAS_CONSTRAINT]->(c:Constraint)
        WHERE c.start <= dayEnd AND c.end >= dayStart   // time overlap
        RETURN collect(DISTINCT {loc: loc, c: c}) AS hits
      }

      WITH n, rcpt, dlv, hits
      WHERE size(hits) > 0
      RETURN
        n.nomId        AS nomId,
        n.pipeline     AS pipeline,
        n.TA           AS TA,
        n.flowDate     AS flowDate,
        n.cycle        AS cycle,
        rcpt.name      AS receiptName,
        n.receiptVolume AS receiptVolume,
        n.fuelLoss     AS fuelLoss,
        dlv.name       AS deliveryName,
        n.deliveryVolume AS deliveryVolume,
        [h IN hits | {
          locationName: h.loc.name,
          locationPipeline: h.loc.pipeline,
          constraintStart: h.c.start,
          constraintEnd: h.c.end,
          constraintPercent: h.c.percent
        }] AS impactedLocations
      ORDER BY nomId
      `,
      { flowDate }
    );
    if (result.records.length === 0) {
      return res.status(404).json({ message: 'Not found' });
    }

    // Map records to plain JS objects
    const nominations = result.records.map(r => {
      const obj = {};
      for (const key of r.keys) {
        obj[key] = toPlain(r.get(key));
      }
      return obj;
    });

    res.json({ flowDate, count: nominations.length, nominations });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /notices/constrained-noms/:locationName/:beforeDate  — fetch all constrained nominations at a location prior to date (YYYY-MM-DD)
// Example: /notices/constrained-noms/SPARTA-MUSKEGON/2025-11-01
app.get('/notices/constrained-noms/:locationName/:beforeDate(\\d{4}-\\d{2}-\\d{2})', async (req, res) => {
  const locationName = req.params.locationName;
  const beforeDate   = req.params.beforeDate;
  const { pipeline } = req.query; // optional

  // Basic input validation
  if (!locationName || typeof locationName !== 'string') {
    return res.status(400).json({ error: "location-name is required" });
  }

  try {
    const result = await runQuery(
      `
      WITH $locationName AS locName,
        date($beforeDate) AS bDate
      
      MATCH (target:Location {name: locName})
      MATCH (rcpt:Location)-[n:NOMINATED]->(dlv:Location)
      WHERE n.flowDate < bDate
        ${pipeline ? 'AND n.pipeline = $pipeline' : ''}

      CALL {
        WITH rcpt, dlv, target
        MATCH p = allShortestPaths( (rcpt)-[:Segment_Locations*]->(dlv) )
        WHERE target IN nodes(p)
        RETURN count(p) > 0 AS passesThroughTarget
      }
      WITH n, rcpt, dlv, target, passesThroughTarget
      WHERE passesThroughTarget

      // Constraint overlap on the nomination's gas day at the target location
      WITH n, rcpt, dlv, target,
          datetime({date: n.flowDate}) AS dayStart,
          datetime({date: n.flowDate}) + duration('P1D') - duration('PT1S') AS dayEnd
      MATCH (target)-[:HAS_CONSTRAINT]->(c:Constraint)
      WHERE c.start <= dayEnd AND c.end >= dayStart

      RETURN
        n.flowDate        AS flowDate,
        n.cycle           AS cycle,
        n.pipeline        AS pipeline,
        n.TA              AS TA,
        rcpt.name         AS receiptLocation,
        n.receiptVolume   AS receiptVolume,
        n.fuelLoss        AS fuelLoss,
        dlv.name          AS deliveryLocation,
        n.deliveryVolume  AS deliveryVolume,
        target.name       AS constrainedLocation,
        c.kind            AS constraintKind,
        c.percent         AS percentConstrained,
        c.start           AS constraintStart,
        c.end             AS constraintEnd
      ORDER BY flowDate DESC, pipeline ASC, TA ASC, cycle DESC
      `,
      { locationName, beforeDate, pipeline }
    );
    if (result.records.length === 0) {
      return res.status(404).json({ message: 'Not found' });
    }

    // Map records to plain JS objects
    const nominations = result.records.map(r => {
      const obj = {};
      for (const key of r.keys) {
        obj[key] = toPlain(r.get(key));
      }
      return obj;
    });

    res.json({ beforeDate, count: nominations.length, nominations });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /volumes/historical-flow/:pipeline/:startDate/:endDate?limit=100&skip=0  — fetch meter volumes for a pipeline and date range w/ pagination
// Example: /volumes/historical-flow/ANR?limit=50&skip=0
app.get('/volumes/historical-flow/:pipeline/:startDate(\\d{4}-\\d{2}-\\d{2})/:endDate(\\d{4}-\\d{2}-\\d{2})', async (req, res) => {
  const pipeline = req.params.pipeline;
  const startDate = req.params.startDate;
  const endDate = req.params.endDate;
  const limit = parseInt(req.query.limit) || 100;
  const skip  = parseInt(req.query.skip)  || 0;
  const locationNumber = parseInt(req.query.locationNumber);  // optional
  const cycle = req.query.cycle; // optional

  try {
    const result = await runQuery(
      `
      MATCH (o:OperationalFlow)
      WHERE o.pipeline = $pipeline AND o.flowDate >= date($startDate) AND o.flowDate <= date($endDate)
        ${locationNumber ? 'AND o.locationNumber = $locationNumber' : ''}
        ${cycle ? 'AND o.cycle = $cycle' : ''}
      RETURN
        o.pipeline              AS pipeline,
        o.locationNumber        AS locationNumber,
        o.flowDate              AS flowDate,
        o.cycle                 AS cycle,
        o.operationalCapacity   AS operationalCapacity,
        o.scheduledVolume       AS scheduledVolume,
        o.utilization           AS utilizationPerCent
      ORDER BY o.pipeline, o.locationNumber, o.flowDate, o.cycle SKIP toInteger($skip) LIMIT toInteger($limit)
      `,
      { pipeline, startDate, endDate, limit, skip, locationNumber, cycle }
    );

    // Map records to plain JS objects
    const flows = result.records.map(r => {
      const obj = {};
      for (const key of r.keys) {
        obj[key] = toPlain(r.get(key));
      }
      return obj;
    });

    res.json({ params: { pipeline, startDate, endDate, locationNumber, cycle }, count: flows.length, flows, page: { skip: Number(skip), limit: Number(limit) } });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /path?from=...&to=...&maxHops= (default unlimited)
// Optionally, pass at=ISO8601 to flag nodes that have an active :Constraint at that instant.
app.get('/path', async (req, res) => {
  const { from, to, maxHops, at } = req.query;
  if (!from || !to) return res.status(400).json({ error: 'from and to are required query params' });
  const hopPattern = maxHops ? `*..${Number(maxHops)}` : '*';
  const atTime = at ? new Date(at).toISOString() : null;

  const cypher = atTime ?
  `MATCH p = (a:Location {name: $from})-[:Segment_Locations${hopPattern}]->(b:Location {name: $to})
   WITH p
   UNWIND nodes(p) AS loc
   OPTIONAL MATCH (loc)-[:HAS_CONSTRAINT]->(c:Constraint)
   WHERE c.start <= datetime($at) AND (c.end IS NULL OR c.end >= datetime($at))
   WITH p, collect(DISTINCT loc {.*, id: id(loc), constrained: count(c) > 0}) AS locs,
        [rel IN relationships(p) | rel {.*, id: id(rel), type: type(rel)}] AS rels
   RETURN locs AS nodes, rels AS relationships
  ` :
  `MATCH p = (a:Location {name: $from})-[:Segment_Locations${hopPattern}]->(b:Location {name: $to})
   RETURN [n IN nodes(p) | n {.*, id: id(n)}] AS nodes,
          [r IN relationships(p) | r {.*, id: id(r), type: type(r)}] AS relationships
  `;

  try {
    const result = await runQuery(cypher, { from, to, at: atTime });
    if (result.records.length === 0) return res.status(404).json({ message: 'No path found' });
    // Return the shortest path result (1st record) to keep payload small
    const { nodes, relationships } = result.records[0].toObject();
    res.json({ nodes, relationships });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /constraints?location=Name&at=ISO&limit=50&skip=0
// If location omitted, returns constraints across graph; if at provided, filters active at time.
app.get('/constraints', async (req, res) => {
  const { location, at, limit = 50, skip = 0 } = req.query;
  const atTime = at ? new Date(at).toISOString() : null;

  const baseMatch = location ?
    `MATCH (l:Location {name: $location})-[:HAS_CONSTRAINT]->(c:Constraint)` :
    `MATCH (c:Constraint)`;

  const timeFilter = atTime ?
    `WHERE c.start <= datetime($at) AND (c.end IS NULL OR c.end >= datetime($at))` : '';

  const cypher = `
    ${baseMatch}
    ${timeFilter}
    WITH c ORDER BY c.start DESC SKIP toInteger($skip) LIMIT toInteger($limit)
    RETURN collect(c {.*, id: id(c)}) AS constraints
  `;

  try {
    const result = await runQuery(cypher, { location, at: atTime, limit: Number(limit), skip: Number(skip) });
    const constraints =
      result.records.length > 0
        ? toPlain(result.records[0].get('constraints'))
        : [];

    res.json({ location, at, count: constraints.length, constraints, page: { skip: Number(skip), limit: Number(limit) } });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /constraints  — create a new Constraint and optionally link to a Location by name
// Body: { reason, kind, start, end?, limit?, locationName? }
app.post('/constraints', async (req, res) => {
  if (!writesEnabled) return res.status(403).json({ error: 'Write endpoints disabled. Set ENABLE_WRITES=true to enable.' });
  const { reason, kind, start, end, limit, locationName } = req.body || {};
  if (!reason || !kind || !start) {
    return res.status(400).json({ error: 'reason, kind and start are required' });
  }
  try {
    const result = await runQuery(`
      CREATE (c:Constraint {
        reason: $reason,
        kind: $kind,
        start: datetime($start),
        end: ${end ? 'datetime($end)' : 'NULL'},
        limit: $limit,
        createdAt: datetime()
      })
      WITH c
      CALL {
        WITH c
        WITH c WHERE $locationName IS NOT NULL
        MATCH (l:Location {name: $locationName})
        MERGE (l)-[:HAS_CONSTRAINT]->(c)
        RETURN l
      }
      RETURN c {.*, id: id(c)} AS constraint
    `, { reason, kind, start, end, limit, locationName }, 'WRITE');
    const { constraint } = result.records[0].toObject();
    res.status(201).json({ constraint });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// PATCH /constraints/set-createdAt-from-start  — one-off utility to set createdAt = start on all Constraint nodes
app.patch('/constraints/set-createdAt-from-start', async (req, res) => {
  if (!writesEnabled) return res.status(403).json({ error: 'Write endpoints disabled. Set ENABLE_WRITES=true to enable.' });
  try {
    const result = await runQuery(`
      MATCH (c:Constraint)
      SET c.createdAt = c.start
      RETURN count(c) AS updated
    `, {}, 'WRITE');
    const { updated } = result.records[0].toObject();
    res.json({ updated });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /cypher/read  — run a parameterized read-only Cypher safely
// Body: { query, params }
// Note: only allows READ; for writes use explicit endpoints above.
app.post('/cypher/read', async (req, res) => {
  const { query, params } = req.body || {};
  if (!query || typeof query !== 'string') return res.status(400).json({ error: 'query is required' });
  // Very light guardrail: disallow write keywords
  const lowered = query.toLowerCase();
  const forbidden = ['create ', 'merge ', 'delete ', 'detach ', 'set ', 'remove ', 'foreach '];
  if (forbidden.some(k => lowered.includes(k))) {
    return res.status(400).json({ error: 'Write operations are not allowed in /cypher/read' });
  }
  try {
    const result = await runQuery(query, params || {}, 'READ');
    const records = result.records.map(r => r.toObject());
    res.json({ records, summary: result.summary.counters.updates() });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ---- Start server ----
app.listen(PORT, () => {
  console.log(`Neo4j API listening on http://localhost:${PORT}`);
});

// Graceful shutdown
process.on('SIGINT', async () => {
  await driver.close();
  process.exit(0);
});
