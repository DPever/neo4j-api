import 'dotenv/config';
import express from 'express';
import neo4j from 'neo4j-driver';
import swaggerUi from 'swagger-ui-express';
import swaggerJsdoc from 'swagger-jsdoc';
import {
  validateDirection,
  validatePosition,
  validateZone,
  validateType
} from './validators/locationValidator.js';
import { ValidationError } from './validators/common.js';
import { validateOacBatch } from './validators/operationallyAvailableCapacityValidator.js'


// ---- Config ----
const {
  NEO4J_URI,
  NEO4J_USERNAME,
  NEO4J_PASSWORD,
  PORT = 8080,
  ENABLE_WRITES = 'true',
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
        Pipeline: {
          type: 'object',
          properties: {
            code: { type: 'string', description: 'Common abbreviation for pipeline' },
            name: { type: 'string', description: 'Pipeline name' },
            operator: { type: 'string', description: 'Company who operates the Pipeline' },
            tspId: { type: 'string', description: 'Transportation Service Provider ID for EDI communications' },
            modelType: { type: 'string', nullable: true, description: 'NAESB Model type (e.g., PNT, PTH, etc.)'}
          }
        },
        Cycle: {
          type: 'object',
          properties: {
            pipelineCode: { type: 'string', description: 'Pipeline code for this cycle' },
            cycleCode: { type: 'string', description: 'Cycle code (e.g., TIM, EVE, ID1, ID2, ID3)' },
            name: { type: 'string', description: 'Cycle name' },
            nomDeadlineLocalTime: { type: 'object', description: 'Nomination deadline in local time'},
            confirmByLocalTime: { type: 'object', description: 'Confirmation deadline in local time' },
            gasDayOffset: { type: 'integer', description: 'Gas day offset (1=next gas day; 0=same gas day)' },
            sortOrder: { type: 'integer', description: 'Sort order for visual display' }
          }
        },
        Zone: {
          type: 'object',
          properties: {
            pipelineCode: { type: 'string', description: 'Pipeline code this zone belongs to' },
            name: { type: 'string', description: 'Zone name' },
            sortOrder: { type: 'integer', description: 'Sort order for visual display' }
          }
        },
        LocationType: {
          type: 'object',
          properties: {
            code: { type: 'string', description: 'Common abbreviation for location type' },
            description: { type: 'string', description: 'Location type description' }
          }
        },
        Position: {
          type: 'object',
          properties: {
            latitude: { type: 'number', description: 'Latitude of the position' },
            longitude: { type: 'number', description: 'Longitude of the position' }
          }
        },
        Location: {
          type: 'object',
          properties: {
            locationId: { type: 'string', description: 'Unique identifier for the location on the pipeline' },
            name: { type: 'string', description: 'Location name' },
            direction: { type: 'string', description: 'Direction of flow at this location' },
            zone: { type: 'string', description: 'Zone this location belongs to' },
            marketArea: { type: 'string', description: 'Market area this location belongs to' },
            type: { type: 'string', description: 'Type of location (e.g., receipt, delivery)' },
            effectiveDate: { type: 'string', format: 'date', description: 'Effective date for this location' },
            endDate: { type: 'string', format: 'date', nullable: true, description: 'End date for this location' },
            state: { type: 'string', description: 'State where the location is located', nullable: true },
            county: { type: 'string', description: 'County where the location is located', nullable: true },
            country: { type:'string', description:'Country where the location is located', nullable:true }, 
            pipelineSegmentCode:{type:'string',description:'Pipeline segment code for the location', nullable:true },
            primaryDataSource:{type:'string',description:'Primary data source for the location'},
            primaryDataAsOf:{type:'string',format:'date-time',description:'Date and time when primary data was as of'},
            position:{type: 'object', $ref: '#/components/schemas/Position', description:'Position of the location', nullable:true },
            positionDataSource:{type:'string',description:'Data source for position information', nullable:true },
            positionDataAsOf:{type:'string',format:'date-time',description:'Date and time when position data was as of', nullable:true }
          }
        },
        TransportationContract : {
          type : "object",
          properties : {
            tbd : { type : "string", description : "To be defined" }
          }
        },
        Notice: {
          type: 'object',
          properties: {
            pipelineCode: { type: 'string' },
            noticeId: { type: 'string' },
            postingDatetime: { type: 'string', format: 'date-time' },
            lastModifiedDatetime: { type: 'string', format: 'date-time' },
            noticeType: { type: 'string' },
            category: { type: 'string' },
            status: { type: 'string' },
            priorNoticeId: { type: 'string', nullable: true },
            subject: { type: 'string' },
            effectiveDatetime: { type: 'string', format: 'date-time' },
            endDatetime: { type: 'string', format: 'date-time', nullable: true },
            content: { type: 'string' }
          }
        },
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
        OperationallyAvailableCapacity: {
          type: 'object',
          properties: {
            pipelineCode: { type: 'string' },
            locationId: { type: 'string' },
            flowDate: { type: 'string', format: 'date' },
            postingDatetime: { type: 'string', format: 'date-time' },
            cycle: { type: 'string' },
            locationName: { type: 'string' },
            locPurpDesc: { type: 'string' },
            locQTI: { type: 'string' },
            direction: { type: 'string' },
            flowIndicator: { type: 'string' },
            grossOrNet: { type: 'string' },
            schedStatus: { type: 'string' },
            designCapacity: { type: 'number' },
            operatingCapacity: { type: 'number' },
            operationallyAvailableCapacity: { type: 'number' },
            totalSchedQty: { type: 'number' },
            itIndicator: { type: 'string' }
          }
        },
        OperationalFlow: {
          type: 'object',
          properties: {
            pipelineCode: { type: 'string' },
            locationId: { type: 'string' },
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

  '/api/v1/pipelines': {
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
                    type: 'array', items: { $ref: '#/components/schemas/Pipeline' } 
                  }
                }
              }
            }
          }
        }
      },
      404: { description: 'Not found' }
    }
  },

  '/api/v1/pipelines/{pipelineCode}': {
    put: {
      summary: 'Update (or Insert) a pipeline where code is the logical key for updates',
      tags: ['Reference Data'],
      parameters: [
      {
        name: 'pipelineCode',
        in: 'path',
        required: true,
        schema: { type: 'string' },
        example: 'ANR'
      }
      ],
      requestBody: {
        required: true,
        content: {
          'application/json': {
            schema: { $ref: '#/components/schemas/Pipeline' },
            example: {
              code: 'ANR',
              name: 'ANR Pipeline',
              operator: 'TC Energy',
              tspId: '006958581',
              modelType: 'PNT'
            }
          }
        }
      },
      responses: {
        200: {
          description: 'Updated',
          content: {
            'application/json': {
              schema: { $ref: '#/components/schemas/Pipeline' }
            }
          }
        },
        400: { description: 'Invalid request body' },
        500: { description: 'Server error' }
      }
    }
  },

  '/api/v1/pipelines/{pipelineCode}/cycles': {
    get: {
      summary: 'Cycles for a pipeline',
      tags: ['Reference Data'],
      parameters: [
        { name: 'pipelineCode', in: 'path', required: true, schema: { type: 'string' }, 
          description: 'Filter by pipeline name (e.g., ANR, TETCO)'
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
                  cycles : {
                    type:'array',
                    items:{ $ref:'#/components/schemas/Cycle'}
                  }
                }
              }
            }
          }
        },
        404:{ description:'Not found'}
      }
    }
  },

  '/api/v1/pipelines/{pipelineCode}/zones': {
    get: {
      summary: 'Zones for a pipeline',
      tags: ['Reference Data'],
      parameters: [
        { name: 'pipelineCode', in: 'path', required: true, schema: { type: 'string' }, 
          description: 'Filter by pipeline name (e.g., ANR, TETCO)'
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
                  zones : {
                    type:'array',
                    items:{ $ref:'#/components/schemas/Zone'}
                  }
                }
              }
            }
          }
        },
        404:{ description:'Not found'}
      }
    }
  },

  '/api/v1/location-types': {
    get: {
      summary: 'List of location types',
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
                  locationTypes : {
                    type:'array',
                    items:{ $ref:'#/components/schemas/LocationType'}
                  }
                }
              }
            }
          }
        },
        404:{ description:'Not found'}
      }
    }
  },

  '/api/v1/pipelines/{pipelineCode}/locations': {
    get: {
      summary: 'Locations for a pipeline',
      tags: ['Reference Data'],
      parameters: [
        { name: 'pipelineCode', in: 'path', required: true, schema: { type: 'string' }, 
          description: 'Filter by pipeline name (e.g., ANR, TETCO)'
        },
        { name: 'asOfDate', in: 'query', required: false, 
          schema: { type: 'string', format: 'date', pattern: '^\\d{4}-\\d{2}-\\d{2}$' },
          example: '2025-11-01'
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
        }
      }
    },
    post: {
      summary: 'Ingest Locations for a pipeline',
      tags: ['Reference Data'],
      parameters: [
        { name: 'pipelineCode', in: 'path', required: true, schema: { type: 'string' }, example: 'ANR' }
      ],
      requestBody: {
        required: true,
        content: {
          'application/json': {
            schema: {
              type: 'object',
              properties: {
                locations: {
                  type: 'array',
                  items: { $ref: '#/components/schemas/Location' }
                }
              }
            }
          }
        }
      },
      responses: {
        201: {
          description: 'Locations ingested',
          content: { 'application/json': { schema: {
            type: 'object',
            properties: {
              locations: { type: 'array', items: { $ref: '#/components/schemas/Location' } }
            }
          } } }
        },
        400: { description: 'Invalid request body' },
        409: { description: 'Conflict - e.g., duplicate location IDs' },
        500: { description: 'Server error' }
      }
    },
  },

  '/api/v1/pipelines/{pipelineCode}/locations/{locationId}': {
    put: {
      summary: 'Update (or Insert) a location where pipelineCode and locationId is the logical key for updates',
      tags: ['Reference Data'],
      parameters: [
        { name: 'pipelineCode', in: 'path', required: true, schema: { type: 'string' }, example: 'ANR' },
        { name: 'locationId', in: 'path', required: true, schema: { type: 'string' }, example: '42078' }
      ],
      requestBody: {
        required: true,
        content: {
          'application/json': {
            schema: { $ref: '#/components/schemas/Location' }
          }
        }
      },
      responses: {
        200: {
          description: 'Location updated or created',
          content: { 'application/json': {
            schema: { 
              type: 'object',
              properties: {
                pipelineCode: { type: 'string' },
                locationId: { type: 'string' },
                outcome: { type: 'string' },
                location: { $ref: '#/components/schemas/Location' }
              }
            } } }
        },
        400: { description: 'Invalid request body' },
        500: { description: 'Server error' }
      }
    },
  },

  '/api/v1/pipelines/{pipelineCode}/connections': {
    get: {
      summary: 'List of connections between locations for a pipeline',
      tags: ['Reference Data'],
      parameters: [
        { name: 'pipelineCode', in: 'path', required: true, schema: { type: 'string' }, 
          description: 'Filter by pipeline name (e.g., ANR, TETCO)'
        },
        { name: 'limit', in: 'query', required: false, schema: { type: 'integer', default: 100 }, 
          description: 'maximum number of connections to return', example: '100'
        },
        { name: 'skip', in: 'query', required: false, schema: { type: 'integer', default: 0 }, 
          description: 'number of connections to skip for pagination', example: '0'
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
                  connections: {
                    type: 'array',
                    items: {
                      type: 'object',
                      properties: {
                        connection: { type: 'object' }
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

  '/api/v1/pipelines/{pipelineCode}/contracts': {
    get: {
      summary: 'Antero firm transportation contracts for a pipeline and as of date',
      tags: ['Reference Data'],
      parameters: [
        { name: 'pipelineCode', in: 'path', required: true, schema: { type: 'string' }, example: 'ANR' },
        { name: 'asOfDate', in: 'query', required: false, 
          schema: { type: 'string', format: 'date', pattern: '^\\d{4}-\\d{2}-\\d{2}$' },
          example: '2025-11-01'
        },
        { name: 'limit', in: 'query', required: false, schema: { type: 'integer', default: 1000 } },
        { name: 'skip', in: 'query', required: false, schema: { type: 'integer', default: 0 } }
      ],
      responses: {
        200: {
          description: 'Firm Transportation Contracts',
          content: { 'application/json': { schema: {
            type: 'object',
            properties: {
              params: { type: 'object' },
              count: { type: 'integer' },
              contracts: { type: 'array', items: { type: 'object' } }
            }
          } } }
        }
      }
    }
  },

  '/api/v1/pipelines/{pipelineCode}/contracts/with-capacity-and-constraints': {
    get: {
      summary: 'Antero firm transportation with capacity and constraints for a pipeline and as of date',
      tags: ['API'],
      parameters: [
        { name: 'pipelineCode', in: 'path', required: true, schema: { type: 'string' }, example: 'ANR' },
        { name: 'asOfDate', in: 'query', required: true, 
          schema: { type: 'string', format: 'date', pattern: '^\\d{4}-\\d{2}-\\d{2}$' },
          example: '2025-11-01'
        },
        { name: 'limit', in: 'query', required: false, schema: { type: 'integer', default: 1000 } },
        { name: 'skip', in: 'query', required: false, schema: { type: 'integer', default: 0 } }
      ],
      responses: {
        200: {
          description: 'Firm Transportation Contracts with Capacity and Constraints',
          content: { 'application/json': { schema: {
            type: 'object',
            properties: {
              params: { type: 'object' },
              count: { type: 'integer' },
              contracts: { type: 'array', items: { type: 'object' } }
            }
          } } }
        }
      }
    }
  },

  '/api/v1/pipelines/{pipelineCode}/path-details/{fromLocationId}/{toLocationId}/{asOfDate}': {
    get: {
      summary: 'Get path details for a pipeline and as of date',
      tags: ['API'],
      parameters: [
        { name: 'pipelineCode', in: 'path', required: true, schema: { type: 'string' }, example: 'ANR' },
        { name: 'fromLocationId', in: 'path', required: true, schema: { type: 'string' }, example: '513105' },
        { name: 'toLocationId', in: 'path', required: true, schema: { type: 'string' }, example: '42078' },
        { name: 'asOfDate', in: 'path', required: true,
          schema: { type: 'string', format: 'date', pattern: '^\\d{4}-\\d{2}-\\d{2}$' },
          example: '2025-11-01'
        }
      ],
      responses: {
        200: {
          description: 'Path Details',
          content: { 'application/json': { schema: {
            type: 'object',
            properties: {
              params: { type: 'object' },
              pathDetails: { type: 'object' }
            }
          } } }
        }
      }
    }
  },
  
  '/api/v1/pipelines/{pipelineCode}/nominations/{flowDate}': {
    get: {
      summary: 'All nominations on a pipeline for a gas day',
      tags: ['Nominations'],
      parameters: [
        { name: 'pipelineCode', in: 'path', required: true, schema: { type: 'string' }, 
          description: 'Filter by pipeline code (e.g., ANR, TETCO)'
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

  '/api/v1/pipelines/{pipelineCode}/agreements/firm-transport': {
    get: {
      summary: 'Firm Transport customer agreements with a pipeline on a date',
      tags: ['Volume'],
      parameters: [
        { name: 'pipelineCode', in: 'path', required: true, schema: { type: 'string' }, example: 'ANR' },
        { name: 'asOfDate', in: 'query', required: true, 
          schema: { type: 'string', format: 'date', pattern: '^\\d{4}-\\d{2}-\\d{2}$' },
          example: '2025-11-01'
        },
        { name: 'shipperName', in: 'query', required: false, schema: { type: 'string' }, example: 'Antero Resources Corporation' },
        { name: 'limit', in: 'query', required: false, schema: { type: 'integer', default: 1000 } },
        { name: 'skip', in: 'query', required: false, schema: { type: 'integer', default: 0 } }
      ],
      responses: {
        200: {
          description: 'Firm Transportation',
          content: { 'application/json': { schema: {
            type: 'object',
            properties: {
              params: { type: 'object' },
              count: { type: 'integer' },
              contracts: { type: 'array', items: { type: 'object' } }
            }
          } } }
        }
      }
    }
  },

  '/api/v1/pipelines/{pipelineCode}/capacities/operationally-available': {
    get: {
      summary: 'Operationally Available Capacity for a pipeline and as of date',
      tags: ['Volume'],
      parameters: [
        { name: 'pipelineCode', in: 'path', required: true, schema: { type: 'string' }, example: 'ANR' },
        { name: 'asOfDate', in: 'query', required: false, 
          schema: { type: 'string', format: 'date', pattern: '^\\d{4}-\\d{2}-\\d{2}$' },
          example: '2025-11-01'
        },
        { name: 'cycle', in: 'query', required: false, schema: { type: 'string' }, description: 'Filter by cycle (e.g., TIM, EVE, ID1, ID2, ID3)' },
        { name: 'locationId', in: 'query', required: false, schema: { type: 'string' } },
        { name: 'limit', in: 'query', required: false, schema: { type: 'integer', default: 1000 }, 
          description: 'maximum number of operationallyAvailableCapacity objects to return', example: '1000'
        },
        { name: 'skip', in: 'query', required: false, schema: { type: 'integer', default: 0 }, 
          description: 'number of operationallyAvailableCapacity objects to skip for pagination', example: '0'
        }
      ],
      responses: {
        200: {
          description: 'Operationally Available Capacity',
          content: { 'application/json': { schema: {
            type: 'object',
            properties: {
              params: { type: 'object' },
              count: { type: 'integer' },
              operationallyAvailableCapacity: { type: 'array', items: { $ref: '#/components/schemas/OperationallyAvailableCapacity' } },
              page: { type: 'object' }
            }
          } } }
        }
      }
    },
    put: {
      summary: 'Ingest Operationally Available Capacity for a pipeline',
      tags: ['Volume'],
      parameters: [
        { name: 'pipelineCode', in: 'path', required: true, schema: { type: 'string' }, example: 'ANR' },
      ],
      requestBody: {
        required: true,
        content: {
          'application/json': {
            schema: {
              type: 'object',
              properties: {
                records: { type: 'array', items: { $ref: '#/components/schemas/OperationallyAvailableCapacity' } }
              }
            }
          }
        }
      },
      responses: {
        200: {
          description: 'Operationally Available Capacity ingested',
          content: { 'application/json': { schema: {
            type: 'object',
            properties: {
                counts: {
                  type: 'object',
                  properties: {
                    received: { type: 'integer' },
                    applied: { type: 'integer' },
                    ignored: { type: 'integer' }
                  }
                }
            }
          } } }
        }
      }
    }
  },

  '/api/v1/pipelines/{pipelineCode}/capacities/cycles-with-data': {
    get: {
      summary: 'Cycles for which Operationally Available Capacity data exists for a pipeline and flow date',
      tags: ['Volume'],
      parameters: [
        { name: 'pipelineCode', in: 'path', required: true, schema: { type: 'string' }, example: 'ANR' },
        { name: 'asOfDate', in: 'query', required: true, 
          schema: { type: 'string', format: 'date', pattern: '^\\d{4}-\\d{2}-\\d{2}$' },
          example: '2026-01-22'
        }
      ],
      responses: {
        200: {
          description: 'Cycles with Operationally Available Capacity',
          content: { 'application/json': { schema: {
            type: 'object',
            properties: {
              params: { type: 'object' },
              count: { type: 'integer' },
              cycles: { type: 'array', items: { type: 'string' } }
            }
          } } }
        }
      }
    },
  },

  '/api/v1/pipelines/{pipelineCode}/flows/{startDate}/{endDate}': {
    get: {
      summary: 'Historic flow volumes and operational capacity at a location for a pipeline and date range',
      tags: ['Volume'],
      parameters: [
        { name: 'pipelineCode', in: 'path', required: true, schema: { type: 'string' }, example: 'ANR' },
        { name: 'startDate',  in: 'path', required: true,
          schema: { type: 'string', format: 'date', pattern: '^\\d{4}-\\d{2}-\\d{2}$' },
          example: '2025-11-01'
        },
        { name: 'endDate',  in: 'path', required: true,
          schema: { type: 'string', format: 'date', pattern: '^\\d{4}-\\d{2}-\\d{2}$' },
          example: '2025-11-30'
        },
        { name: 'locationId', in: 'query', required: false, schema: { type: 'string' } },
        { name: 'cycle', in: 'query', required: false, schema: { type: 'string' }, description: 'Filter by cycle (e.g., TIM, EVE, ID1, ID2, ID3)' },
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

  '/api/v1/pipelines/{pipelineCode}/prices/{startDate}/{endDate}': {
    get: {
      summary: 'Redion based prices for a pipeline and date range',
      tags: ['Prices'],
      parameters: [
        { name: 'pipelineCode', in: 'path', required: true, schema: { type: 'string' }, example: 'ANR' },
        { name: 'startDate',  in: 'path', required: true,
          schema: { type: 'string', format: 'date', pattern: '^\\d{4}-\\d{2}-\\d{2}$' },
          example: '2025-11-01'
        },
        { name: 'endDate',  in: 'path', required: true,
          schema: { type: 'string', format: 'date', pattern: '^\\d{4}-\\d{2}-\\d{2}$' },
          example: '2025-11-30'
        },
        { name: 'limit', in: 'query', required: false, schema: { type: 'integer', default: 100 }, 
          description: 'maximum number of flow rows to return', example: '100'
        },
        { name: 'skip', in: 'query', required: false, schema: { type: 'integer', default: 0 }, 
          description: 'number of flow rows to skip for pagination', example: '0'
        }
      ],
      responses: {
        200: {
          description: 'Prices',
          content: { 'application/json': { schema: {
            type: 'object',
            properties: {
              params: { type: 'object' },
              count: { type: 'integer' },
              prices: { type: 'array', items: { type: 'object' } },
              page: { type: 'object' }
            }
          } } }
        }
      }
    }
  },

  '/api/v1/prices/symbol-trading-day': {
    put: {
      summary: 'Ingest prices for a symbol (aka: ticker) and trading day',
      tags: ['Prices'],
      requestBody: {
        required: true,
        content: {
          'application/json': {
            schema: {
              type: 'object',
              properties: {
                prices: {
                  type: 'array',
                  items: { type: 'object',
                    properties: {
                      symbol: { type: 'string' },
                      tradingDay: { type: 'string', format: 'date' },
                      high: { type: 'number' },
                      low: { type: 'number' },
                      mid: { type: 'number' },
                      close: { type: 'number' },
                      volume: { type: 'integer' },
                      modificationDatetime: { type: 'string', format: 'date-time' }
                   }
                  }
                }
              }
            }
          }
        },
      },
      responses: {
        200: {
          description: 'Prices ingested',
          content: { 'application/json': { schema: {
            type: 'object',
            properties: {
                counts: {
                  type: 'object',
                  properties: {
                    received: { type: 'integer' },
                    created: { type: 'integer' },
                    updated: { type: 'integer' },
                    failedUnknownSymbol: { type: 'integer' }
                  }
                }
            }
          } } }
        }
      }
    }
  },

  // DEPRECATED - to be removed in future releases
  '/notices/constrained-noms/{locationName}/{beforeDate}': {
    get: {
      summary: 'DEPRECATED: Nominations that pass through a location and had prior constraints before a date',
      deprecated: true,
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
  },

  '/api/v1/notices/{pipeline}': {
    post: {
      summary: 'Ingest notices for a pipeline',
      tags: ['Notices'],
      parameters: [
        { name: 'pipeline', in: 'path', required: true, schema: { type: 'string' }, example: 'ANR' }
      ],
      requestBody: {
        required: true,
        content: {
          'application/json': {
            schema: {
              type: 'object',
              properties: {
                notices: {
                  type: 'array',
                  items: { $ref: '#/components/schemas/Notice' }
                }
              }
            }
          }
        }
      },
      responses: {
        200: {
          description: 'Notices ingested',
          content: { 'application/json': { schema: {
            type: 'object',
            properties: {
              notices: { type: 'array', items: { $ref: '#/components/schemas/Notice' } }
            }
          } } }
        }
      }
    }
  },

  '/api/v1/pipelines/{pipelineCode}/notices': {
    get: {
      summary: 'Notices on a pipeline within a given date range',
      tags: ['Notices'],
      parameters: [
        { name: 'pipelineCode', in: 'path', required: true, schema: { type: 'string' }, example: 'ANR' },
        { name: 'startDate',  in: 'query', required: true,
          schema: { type: 'string', format: 'date', pattern: '^\\d{4}-\\d{2}-\\d{2}$' },
          example: '2025-11-01'
        },
        { name: 'endDate',  in: 'query', required: true,
          schema: { type: 'string', format: 'date', pattern: '^\\d{4}-\\d{2}-\\d{2}$' },
          example: '2025-11-30'
        },
        { name: 'category', in: 'query', required: false, schema: { type: 'string' }, description: 'category (e.g., Critical, Non-Critical)' },
        { name: 'noticeType', in: 'query', required: false, schema: { type: 'string' }, description: 'noticeType (e.g., Capacity Constraint, Curtailment, Force Majeure)' },
        { name: 'limit', in: 'query', required: false, schema: { type: 'integer', default: 100 }, 
          description: 'maximum number of flow rows to return', example: '100'
        },
        { name: 'skip', in: 'query', required: false, schema: { type: 'integer', default: 0 }, 
          description: 'number of flow rows to skip for pagination', example: '0'
        }
      ],
      responses: {
        200: {
          description: 'Notices',
          content: { 'application/json': { schema: {
            type: 'object',
            properties: {
              pipeline: { type: 'string' },
              asOf: { type: 'string', format: 'date-time' },
              count: { type: 'integer' },
              notices: { type: 'array', items: { $ref: '#/components/schemas/Notice' } }
            }
          } } }
        }
      }
    }
  },

  '/api/v1/pipelines/{pipelineCode}/notices/{noticeId}': {
    get: {
      summary: 'Notice on a pipeline with all Locations impacted',
      tags: ['Notices'],
      parameters: [
        { name: 'pipelineCode', in: 'path', required: true, schema: { type: 'string' }, example: 'ANR' },
        { name: 'noticeId', in: 'path', required: true, schema: { type: 'string' }, example: '12345' }
      ],
      responses: {
        200: {
          description: 'Notice Details',
          content: { 'application/json': { schema: {
            type: 'object',
            properties: {
              pipelineCode: { type: 'string' },
              notice: { type: 'object' }
            }
          } } }
        }
      }
    }
  },

  '/api/v1/pipelines/{pipelineCode}/constraints': {
    get: {
      summary: 'Constraints on a pipeline, optionally filtered by location and time',
      tags: ['Constraints'],
      parameters: [
        { name: 'pipelineCode', in: 'path', required: true, schema: { type: 'string' }, example: 'ANR' },
        { name: 'location', in: 'query', required: false, schema: { type: 'string' }, description: 'Location name (e.g., SPARTA-MUSKEGON)' },
        { name: 'asOf',  in: 'query', required: false,
          schema: { type: 'string', pattern: '^\\d{4}-\\d{2}-\\d{2}(?:T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?(?:Z|[+-]\\d{2}:\\d{2})?)?$' },
          examples: {
            date: { value: '2025-11-01' },
            dateTime: { value: '2025-11-01T15:30:00Z' }
          }
        },
        { name: 'limit', in: 'query', required: false, schema: { type: 'integer', default: 100 }, 
          description: 'maximum number of flow rows to return', example: '100'
        },
        { name: 'skip', in: 'query', required: false, schema: { type: 'integer', default: 0 }, 
          description: 'number of flow rows to skip for pagination', example: '0'
        }
      ],
      responses: {
        200: {
          description: 'Constraints',
          content: { 'application/json': { schema: {
            type: 'object',
            properties: {
              beforeDate: { type: 'string', format: 'date' },
              count: { type: 'integer' },
              constraints: { type: 'array', items: { $ref: '#/components/schemas/Constraint' } }
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

app.get('/warm', async (req, res) => {
    res.json({ status: 'ok' }); // simple endpoint to keep the demo server warm
});

app.get('/health', async (req, res) => {
  try {
    await driver.getServerInfo(); // lightweight handshake
    res.json({ status: 'ok' });
  } catch (e) {
    res.status(500).json({ status: 'error', error: e.message });
  }
});

// GET /api/v1/pipelines  — fetch all pipelines
app.get('/api/v1/pipelines', async (req, res) => {
  try {
    const result = await runQuery(
      `
      MATCH (n:Pipeline) 
      RETURN
        n.code        as code,
        n.name        as name,
        n.operator    as operator,
        n.tspId       as tspId,
        n.modelType   as modelType
      ORDER BY n.code
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

// PUT /api/v1/pipelines/:pipelineCode  — upsert a single pipeline
app.put('/api/v1/pipelines/:pipelineCode', async (req, res) => {
  const code = req.params.pipelineCode;
  const { code: bodyCode, name, operator, tspId, modelType } = req.body || {};

  // if code is sent in the body, it must match the path param to avoid ambiguity
  if (bodyCode && bodyCode !== code) {
    return res.status(400).json({
      error: `Path code '${code}' does not match body code '${bodyCode}'`
    });
  }

  // Basic validation against your Pipeline schema
  if (typeof name !== 'string' || typeof operator !== 'string' || typeof tspId !== 'string'
      || (modelType !== null && typeof modelType !== 'string')) 
  {
    return res.status(400).json({
      error: 'Invalid body. Expected: { name: string, operator: string, tspId: string, modelType?: string | null }'
    });
  }

  try {
    const result = await runQuery(
      `
      MERGE (p:Pipeline {code: $code})
      SET
        p.name     = $name,
        p.operator = $operator,
        p.tspId    = $tspId
        ${modelType ? ' , p.modelType = $modelType ' : ''}
      RETURN
        p.code     AS code,
        p.name     AS name,
        p.operator AS operator,
        p.tspId    AS tspId,
        p.modelType AS modelType
      `,
      { code, name, operator, tspId, modelType }, 'WRITE'
    );

    const record = result.records[0];
    const pipeline = {};
    for (const key of record.keys) {
      pipeline[key] = toPlain(record.get(key));
    }

    res.json(pipeline);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/v1/pipelines/:pipelineCode/cycles  — fetch all cycles for a given pipeline
// Example: /api/v1/pipelines/ANR/cycles
app.get('/api/v1/pipelines/:pipelineCode/cycles', async (req, res) => {
  const pipeline = req.params.pipelineCode;
  
  // Basic input validation
  if (!pipeline || typeof pipeline !== 'string') {
    return res.status(400).json({ error: "pipelineCode is required" });
  }
  try {
    const result = await runQuery(
      `
      MATCH (c:Cycle)
      WHERE c.pipelineCode = $pipeline
      RETURN
        c.pipelineCode          AS pipelineCode,
        c.cycleCode             AS cycleCode,
        c.name                  AS name,
        c.nomDeadlineLocalTime  AS nomDeadlineLocalTime,
        c.confirmByLocalTime    AS confirmByLocalTime,
        c.gasDayOffset          AS gasDayOffset,
        c.sortOrder             AS sortOrder
      ORDER BY c.sortOrder
      `,
      { pipeline }
    );

    // Map records to plain JS objects
    const cycles = result.records.map(r => {
      const obj = {};
      for (const key of r.keys) {
        obj[key] = toPlain(r.get(key));
      }
      return obj;
    });

    res.json({ count: cycles.length, pipeline,  cycles });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/v1/pipelines/:pipelineCode/zones  — fetch all zones for a given pipeline
// Example: /api/v1/pipelines/ANR/zones
app.get('/api/v1/pipelines/:pipelineCode/zones', async (req, res) => {
  const pipeline = req.params.pipelineCode;

  // Basic input validation
  if (!pipeline || typeof pipeline !== 'string') {
    return res.status(400).json({ error: "pipeline is required" });
  }
  try {
    const result = await runQuery(
      `
      MATCH (z:Zone)
      WHERE z.pipelineCode = $pipeline
      RETURN
        z.pipelineCode AS pipelineCode,
        z.name         AS name,
        z.sortOrder    AS sortOrder
      ORDER BY z.sortOrder
      `,
      { pipeline }
    );

    // Map records to plain JS objects
    const zones = result.records.map(r => {
      const obj = {};
      for (const key of r.keys) {
        obj[key] = toPlain(r.get(key));
      }
      return obj;
    });

    res.json({ count: zones.length, pipeline,  zones });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/v1/location-types  — fetch all location types
app.get('/api/v1/location-types', async (req, res) => {
  try {
    const result = await runQuery(
      `
      MATCH (n:LocationType) 
      RETURN
        n.code        as code,
        n.description as description
      ORDER BY n.code
      `
    );

    // Map records to plain JS objects
    const locationTypes = result.records.map(r => {
      const obj = {};
      for (const key of r.keys) {
        obj[key] = toPlain(r.get(key));
      }
      return obj;
    });

    res.json({ count: locationTypes.length, locationTypes });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/v1/pipelines/:pipelineCode/locations?limit=100&skip=0  — fetch all Locations for a given pipeline w/ pagination
// Example: /api/v1/pipelines/ANR/locations?asOfDate=2025-11-01&limit=50&skip=0
app.get('/api/v1/pipelines/:pipelineCode/locations', async (req, res) => {
  const pipeline = req.params.pipelineCode;
  const asOfDate = req.query.asOfDate;
  const limit = parseInt(req.query.limit) || 1000;
  const skip  = parseInt(req.query.skip)  || 0;

  // Basic input validation
  if (!pipeline || typeof pipeline !== 'string') {
    return res.status(400).json({ error: "pipelineCode is required" });
  }
  if (asOfDate && !/^\d{4}-\d{2}-\d{2}$/.test(asOfDate)) {
    return res.status(400).json({ error: "asOfDate must be in YYYY-MM-DD format" });
  }
  try {
    const result = await runQuery(
      `
      MATCH (l:Location)
      WHERE l.pipelineCode = $pipeline
        ${asOfDate ? 'AND l.effectiveDate <= date($asOfDate) AND (l.endDate IS NULL OR date($asOfDate) <= l.endDate)' : ''}
      RETURN
        l.pipelineCode AS pipelineCode,
        l.locationId  AS locationId,
        l.name        AS name,
        l.direction   AS direction,
        l.type        AS type,
        l.zone        AS zone,
        l.marketArea  AS marketArea,
        l.effectiveDate AS effectiveDate,
        l.endDate     AS endDate,
        l.state       AS state,
        l.county      AS county,
        l.pipelineSegmentCode AS pipelineSegmentCode,
        l.primaryDataSource AS primaryDataSource,
        l.primaryDataAsOf AS primaryDataAsOf,
        l.position AS position,
        l.positionDataSource      AS positionDataSource,
        l.positionDataAsOf        AS positionDataAsOf
      ORDER BY l.pipelineCode, l.locationId SKIP toInteger($skip) LIMIT toInteger($limit)
      `,
      { pipeline, asOfDate, limit, skip }
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

// POST /api/v1/pipelines/:pipelineCode/locations  — create a new Location on a pipeline
// Body: { locationId, name, direction, zone, marketArea, type, effectiveDate, endDate?, state?, county?, country?, pipelineSegmentCode?, primaryDataSource, primaryDataAsOf, position?, positionDataSource?, positionDataAsOf? }
app.post('/api/v1/pipelines/:pipelineCode/locations', async (req, res) => {
  const pipelineCode = req.params.pipelineCode;
  const body = req.body ?? {};
  
  // Accept either: { ...location } OR { locations: [ ... ] }
  const locations = Array.isArray(body.locations) ? body.locations : [body];

  if (locations.length === 0) {
    return res.status(400).json({ error: 'No locations provided' });
  }

  // Required fields
  const required = [
    'locationId',
    'name',
    'direction',
    'zone',
    'marketArea',
    'type',
    'effectiveDate',
    'primaryDataSource',
    'primaryDataAsOf'
  ];

const optStr = (v) => {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  return s === '' ? null : s;
};

const normalize = (n) => {
  for (const k of required) {
    if (n[k] === undefined || n[k] === null || String(n[k]).trim() === '') {
      throw new Error(`Missing required field: ${k}`);
    }
  }

  return {
    locationId: String(n.locationId).trim(),
    name: String(n.name).trim(),
    direction: String(n.direction).trim(),
    zone: String(n.zone).trim(),
    marketArea: String(n.marketArea).trim(),
    type: String(n.type).trim(),

    // Expect ISO date strings like "2026-01-11"
    effectiveDate: String(n.effectiveDate).trim(),
    endDate: optStr(n.endDate),

    state: optStr(n.state),
    county: optStr(n.county),
    country: optStr(n.country),
    pipelineSegmentCode: optStr(n.pipelineSegmentCode),

    primaryDataSource: String(n.primaryDataSource).trim(),

    // Expect ISO datetime like "2026-01-09T11:20:00Z" (or without Z if you prefer local)
    primaryDataAsOf: String(n.primaryDataAsOf).trim(),

    // If you want position as a Neo4j point, pass { latitude, longitude } as an object (see note below)
    position: n.position ?? null,

    positionDataSource: optStr(n.positionDataSource),
    positionDataAsOf: optStr(n.positionDataAsOf)
  };
};

  let normalized;
  try {
    normalized = locations.map(normalize);
  } catch (e) {
    return res.status(400).json({ error: e.message });
  }

  // validate fields against enums and existing zones
  const validZones = await fetchZoneNamesForPipeline(runQuery, pipelineCode);
  const validTypes = await fetchLocationTypeCodesForPipeline(runQuery);
  try {
    for (const loc of normalized) {
      validateDirection(loc.direction);
      validatePosition(loc.position);
      validateZone(loc.zone, validZones, pipelineCode);
      validateType(loc.type, validTypes, pipelineCode);
    }
  } catch (e) {
    return res.status(400).json({ error: e.message, details: e.details });
  }

  const cypher = `
    UNWIND $locations AS location
    CREATE (n:Location)
    SET
      n.pipelineCode = $pipelineCode,
      n.locationId = location.locationId,
      n.name = location.name,
      n.direction = location.direction,
      n.zone = location.zone,
      n.marketArea = location.marketArea,
      n.type = location.type,
      n.effectiveDate = date(location.effectiveDate),
      n.endDate = CASE
        WHEN location.endDate IS NULL THEN NULL
        ELSE date(location.endDate)
      END,
      n.state = location.state,
      n.county = location.county,
      n.country = location.country,
      n.pipelineSegmentCode = location.pipelineSegmentCode,
      n.primaryDataSource = location.primaryDataSource,
      n.primaryDataAsOf = datetime(location.primaryDataAsOf),
      n.position = CASE
        WHEN location.position IS NULL THEN NULL
        ELSE point(location.position)
      END,
      n.positionDataSource = location.positionDataSource,
      n.positionDataAsOf = CASE
        WHEN location.positionDataAsOf IS NULL THEN NULL
        ELSE datetime(location.positionDataAsOf)
      END,
      n.createdAt = datetime(),
      n.updatedAt = datetime()
    RETURN n
  `;

  try {
    const result = await runQuery(cypher, { pipelineCode, locations: normalized }, 'WRITE');
    const created = result.records.map(r => toPlain(r.get('n').properties));
    return res.status(201).json({ pipelineCode, count: created.length, locations: created });
  } catch (e) {
    if (String(e.code || '').includes('ConstraintValidationFailed')) {
      return res.status(409).json({ error: 'One or more locations already exist (constraint violation)' });
    }
    return res.status(500).json({ error: e.message });
  }
});

// PUT /api/v1/pipelines/:pipelineCode/locations/:locationId — upsert a single Location
// Body: { name, direction, zone, marketArea, type, effectiveDate, endDate?, state?, county?, country?, pipelineSegmentCode?, primaryDataSource, primaryDataAsOf, position?, positionDataSource?, positionDataAsOf? }
// Note: locationId is taken from route; if provided in body, must match.
app.put('/api/v1/pipelines/:pipelineCode/locations/:locationId', async (req, res) => {
  const pipelineCode = String(req.params.pipelineCode || '').trim().toUpperCase();
  const locationIdParam = String(req.params.locationId || '').trim();

  if (!pipelineCode) return res.status(400).json({ error: 'Missing pipelineCode in route' });
  if (!locationIdParam) return res.status(400).json({ error: 'Missing locationId in route' });

  const body = req.body ?? {};

  const required = [
    'name',
    'direction',
    'zone',
    'marketArea',
    'type',
    'effectiveDate',
    'primaryDataSource',
    'primaryDataAsOf'
  ];

  const optStr = (v) => {
    if (v === undefined || v === null) return null;
    const s = String(v).trim();
    return s === '' ? null : s;
  };

  const normalize = (n) => {
    // If client includes locationId in body, require it matches the route
    const bodyLocationId = optStr(n.locationId);
    if (bodyLocationId && bodyLocationId !== locationIdParam) {
      throw new Error(`Body locationId '${bodyLocationId}' does not match route locationId '${locationIdParam}'`);
    }

    for (const k of required) {
      if (n[k] === undefined || n[k] === null || String(n[k]).trim() === '') {
        throw new Error(`Missing required field: ${k}`);
      }
    }

    return {
      // Identity from route
      locationId: locationIdParam,

      name: String(n.name).trim(),
      direction: String(n.direction).trim(),
      zone: String(n.zone).trim(),
      marketArea: String(n.marketArea).trim(),
      type: String(n.type).trim(),

      effectiveDate: String(n.effectiveDate).trim(),
      endDate: optStr(n.endDate),

      state: optStr(n.state),
      county: optStr(n.county),
      country: optStr(n.country),
      pipelineSegmentCode: optStr(n.pipelineSegmentCode),

      primaryDataSource: String(n.primaryDataSource).trim(),
      primaryDataAsOf: String(n.primaryDataAsOf).trim(),

      position: n.position ?? null, // { latitude, longitude } or null

      positionDataSource: optStr(n.positionDataSource),
      positionDataAsOf: optStr(n.positionDataAsOf)
    };
  };

  let loc;
  try {
    loc = normalize(body);
  } catch (e) {
    return res.status(400).json({ error: e.message });
  }

  // validate fields against enums and existing zones
  const validZones = await fetchZoneNamesForPipeline(runQuery, pipelineCode);
  const validTypes = await fetchLocationTypeCodesForPipeline(runQuery);
  try {
    validateDirection(loc.direction);
    validatePosition(loc.position);
    validateZone(loc.zone, validZones, pipelineCode);
    validateType(loc.type, validTypes, pipelineCode);
  } catch (e) {
    return res.status(400).json({ error: e.message, details: e.details });
  }

  const cypher = `
    MERGE (n:Location { pipelineCode: $pipelineCode, locationId: $locationId })
    ON CREATE SET
      n.createdAt = datetime(),
      n._wasCreated = true
    WITH n, coalesce(n._wasCreated, false) AS wasCreated

    SET
      n.name = $name,
      n.direction = $direction,
      n.zone = $zone,
      n.marketArea = $marketArea,
      n.type = $type,
      n.effectiveDate = date($effectiveDate),
      n.endDate = CASE WHEN $endDate IS NULL THEN NULL ELSE date($endDate) END,
      n.state = $state,
      n.county = $county,
      n.country = $country,
      n.pipelineSegmentCode = $pipelineSegmentCode,
      n.primaryDataSource = $primaryDataSource,
      n.primaryDataAsOf = datetime($primaryDataAsOf),
      n.position = CASE WHEN $position IS NULL THEN NULL ELSE point($position) END,
      n.positionDataSource = $positionDataSource,
      n.positionDataAsOf = CASE WHEN $positionDataAsOf IS NULL THEN NULL ELSE datetime($positionDataAsOf) END,
      n.updatedAt = datetime()

    REMOVE n._wasCreated

    RETURN
      n AS node,
      CASE WHEN wasCreated THEN 'CREATED' ELSE 'UPDATED' END AS outcome
  `;

  try {
    const params = { pipelineCode, locationId: loc.locationId, ...loc };
    const result = await runQuery(cypher, params, 'WRITE');

    const record = result.records[0];
    const outcome = record.get('outcome');
    const location = toPlain(record.get('node').properties);

    // Keep it simple: 200 for both outcomes
    return res.status(200).json({ pipelineCode, locationId: loc.locationId, outcome, location });
  } catch (e) {
    // With your unique constraint, this should be rare here (MERGE), but keep defensive handling
    if (String(e.code || '').includes('ConstraintValidationFailed')) {
      return res.status(409).json({ error: 'Constraint violation' });
    }
    return res.status(500).json({ error: e.message });
  }
});

// GET /api/v1/pipelines/:pipelineCode/connections?limit=100&skip=0  — fetch all connections for a given pipeline w/ pagination
// Example: /api/v1/pipelines/ANR/connections?limit=50&skip=0
app.get('/api/v1/pipelines/:pipelineCode/connections', async (req, res) => {
  const pipeline = req.params.pipelineCode;
  const limit = parseInt(req.query.limit) || 100;
  const skip  = parseInt(req.query.skip)  || 0;

  // Basic input validation
  if (!pipeline || typeof pipeline !== 'string') {
    return res.status(400).json({ error: "pipeline is required" });
  }
  try {
    const result = await runQuery(
      `
      MATCH (src:Location)-[r:CONNECTS_TO]->(dst:Location)
      WHERE r.pipelineCode = $pipeline
      RETURN 
        src.name AS sourceName, src.locationId as sourceLocationId,
        dst.name AS destName, dst.locationId as destLocationId
      ORDER BY src.position.y DESC SKIP toInteger($skip) LIMIT toInteger($limit)
      `,
      { pipeline, limit, skip }
    );

    // Map records to plain JS objects
    const connections = result.records.map(r => {
      const obj = {};
      for (const key of r.keys) {
        obj[key] = toPlain(r.get(key));
      }
      return obj;
    });

    res.json({ pipeline, count: connections.length, connections, page: { skip: Number(skip), limit: Number(limit) } });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/v1/pipelines/:pipelineCode — fetch Antero's firm transport for a pipeline and date
// Example: /api/v1/pipelines/ANR?asOfDate=2025-11-01
app.get('/api/v1/pipelines/:pipelineCode/contracts', async (req, res) => {
  const pipeline = req.params.pipelineCode;
  const asOfDate = req.query.asOfDate; // optional
  const limit = parseInt(req.query.limit) || 1000;
  const skip = parseInt(req.query.skip) || 0;

  try {
    const result = await runQuery(
      `
      MATCH (tc:TransportationContract {pipelineCode: $pipeline})
      MATCH (tc)-[:HAS_SEASON]->(cs:ContractSeason)
      WHERE tc.rateSchedule STARTS WITH 'FT'  // only firm transport
        ${asOfDate ? 'AND tc.effectiveDate <= date($asOfDate) <= tc.endDate AND cs.effectiveDate <=  date($asOfDate) <= cs.endDate' : ''}
        
      MATCH (cs)-[:PRIMARY_RECEIPT]->(rec:Location)
      MATCH (cs)-[:PRIMARY_DELIVERY]->(del:Location)
      RETURN
        tc.pipelineCode       AS pipelineCode,
        tc.shipperName        AS shipperName,
        tc.rateSchedule       AS rateSchedule,
        tc.contractId         AS contractId,
        tc.effectiveDate      AS contractEffectiveDate,
        tc.endDate            AS contractEndDate,
        tc.baseMDQ            AS baseMDQ,
        tc.flowUnit           AS flowUnit,
        cs.seasonId           AS seasonId,
        cs.effectiveDate      AS seasonEffectiveFrom,
        cs.endDate            AS seasonEffectiveTo,
        cs.mdq                AS mdq,
        rec.name              AS primaryReceipt,
        rec.locationId        AS primaryReceiptLocationId,
        del.name              AS primaryDelivery,
        del.locationId        AS primaryDeliveryLocationId
      ORDER BY tc.shipperName, tc.rateSchedule, tc.contractId SKIP toInteger($skip) LIMIT toInteger($limit);
      `,
      { pipeline, asOfDate, limit, skip }
    );

    // Map records to plain JS objects
    const contracts = result.records.map(r => {
      const obj = {};
      for (const key of r.keys) {
        obj[key] = toPlain(r.get(key));
      }
      return obj;
    });

    res.json({ params: { pipeline, asOfDate }, count: contracts.length, contracts });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/v1/pipelines/:pipelineCode/contracts/with-capacity-and-constraints — fetch Antero's firm transport with capacity and constraints info
// Example: /api/v1/pipelines/ANR/contracts/with-capacity-and-constraints?asOfDate=2025-11-01
app.get('/api/v1/pipelines/:pipelineCode/contracts/with-capacity-and-constraints', async (req, res) => {
  const pipeline = req.params.pipelineCode;
  const asOfDate = req.query.asOfDate;
  const limit = parseInt(req.query.limit) || 1000;
  const skip = parseInt(req.query.skip) || 0;

  // Basic input validation
  if (!asOfDate || !/^\d{4}-\d{2}-\d{2}$/.test(asOfDate)) {
    return res.status(400).json({ error: "asOfDate is required and must be in YYYY-MM-DD format" });
  }

  try {
    const result = await runQuery(
      `
      MATCH (tc:TransportationContract {pipelineCode: $pipeline})
      MATCH (tc)-[:HAS_SEASON]->(cs:ContractSeason)
      WHERE tc.rateSchedule STARTS WITH 'FT'  // only firm transport
        AND tc.effectiveDate <= date($asOfDate) <= tc.endDate AND cs.effectiveDate <=  date($asOfDate) <= cs.endDate
        
      MATCH (cs)-[:PRIMARY_RECEIPT]->(rec:Location)
      MATCH (cs)-[:PRIMARY_DELIVERY]->(del:Location)

      // Find shortest path between receipt and delivery
      OPTIONAL MATCH path = shortestPath((rec)-[:CONNECTS_TO*..200]->(del))
      
      // collect locations along the path that have constraints effective on asOfDate
      WITH tc, cs, rec, del,
        CASE WHEN path IS NULL THEN [] ELSE nodes(path) END AS pathLocs

      UNWIND pathLocs AS loc
      OPTIONAL MATCH (loc)-[:HAS_CONSTRAINT]->(c:Constraint)
      WHERE c.effectiveDatetime <= datetime($asOfDate) <= c.endDatetime
      
      WITH tc, cs, rec, del,
        collect(DISTINCT CASE
          WHEN c IS NULL THEN NULL
          ELSE {
            locationId:     loc.locationId,
            locationName:   loc.name,
            kind:           c.kind,
            limit:          c.limit,
            percent:        c.percent,
            flowUnit:       c.units,
            effectiveDatetime:  c.effectiveDatetime,
            endDatetime:    c.endDatetime
          }
        END) AS rawConstraints

      WITH tc, cs, rec, del,
        [x IN rawConstraints WHERE x IS NOT NULL] AS constraints

      RETURN
        tc.pipelineCode       AS pipelineCode,
        tc.shipperName        AS shipperName,
        tc.rateSchedule       AS rateSchedule,
        tc.contractId         AS contractId,
        tc.effectiveDate      AS contractEffectiveDate,
        tc.endDate            AS contractEndDate,
        tc.baseMDQ            AS baseMDQ,
        tc.flowUnit           AS flowUnit,
        cs.seasonId           AS seasonId,
        cs.effectiveDate      AS seasonEffectiveFrom,
        cs.endDate            AS seasonEffectiveTo,
        cs.mdq                AS mdq,
        rec.name              AS primaryReceipt,
        rec.locationId        AS primaryReceiptLocationId,
        del.name              AS primaryDelivery,
        del.locationId        AS primaryDeliveryLocationId,
        constraints          AS constraints
      ORDER BY tc.shipperName, tc.rateSchedule, tc.contractId SKIP toInteger($skip) LIMIT toInteger($limit);
      `,
      { pipeline, asOfDate, limit, skip }
    );

    // Map records to plain JS objects
    const contracts = result.records.map(r => {
      const obj = {};
      for (const key of r.keys) {
        obj[key] = toPlain(r.get(key));
      }
      return obj;
    });

    // Enrich contracts with scheduled quantity
    const scheduledPairs = await mapWithConcurrency(
      contracts,
      5, // <= tune (5–10 is usually plenty)
      async (c) => {
        const qty = await getScheduledQty(c.contractId, asOfDate);
        return [c.contractId, qty ?? 0];
      }
    );

    const scheduledByContractId = Object.fromEntries(scheduledPairs);


    // Enrich contracts with calculated maximum available capacity
    const capacityPairs = await mapWithConcurrency(
      contracts,
      5,
      async (c) => {
        const capacity = await calculateMaxCapacity(c, asOfDate);
        return [c.contractId, capacity ?? 0];
      }
    );

    const capacityByContractId = Object.fromEntries(capacityPairs);

    const enrichedContracts = contracts.map(c => ({
      ...c,
      scheduledQty: scheduledByContractId[c.contractId] ?? 0,
      calculatedMaxCapacity: capacityByContractId[c.contractId] ?? 0
    }));

    const contractsWithLocationCapacity = await mapWithConcurrency(
      enrichedContracts,
      5, // keep modest — this doubles calls
      async (c) => {
        const [
          receiptResult,
          deliveryResult
        ] = await Promise.all([
          getCapacityAndUtilizationAtLocation(
            pipeline,
            c.primaryReceiptLocationId,
            'RPQ',
            asOfDate, 1
          ),
          getCapacityAndUtilizationAtLocation(
            pipeline,
            c.primaryDeliveryLocationId,
            'DPQ',
            asOfDate, 1
          )
        ]);

        return {
          ...c,
          primaryReceiptCapacity: receiptResult.capacity[0] ?? null,
          primaryDeliveryCapacity: deliveryResult.capacity[0] ?? null
        };
      }
    );

    res.json({ params: { pipeline, asOfDate }, count: enrichedContracts.length, contractsWithLocationCapacity });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/v1/pipelines/:pipelineCode/path-details/:fromLocationId/:toLocationId/:asOfDate
// Example: /api/v1/pipelines/ANR/path-details/513105/42078/2025-11-01
app.get(
  '/api/v1/pipelines/:pipelineCode/path-details/:fromLocationId/:toLocationId/:asOfDate(\\d{4}-\\d{2}-\\d{2})',
  async (req, res) => {
    const { pipelineCode, fromLocationId, toLocationId, asOfDate } = req.params;

    // Basic input validation
    if (!pipelineCode) return res.status(400).json({ error: 'pipelineCode is required' });
    if (!fromLocationId) return res.status(400).json({ error: 'fromLocationId is required' });
    if (!toLocationId) return res.status(400).json({ error: 'toLocationId is required' });
    if (!asOfDate || !/^\d{4}-\d{2}-\d{2}$/.test(asOfDate)) {
      return res.status(400).json({ error: 'asOfDate is required and must be in YYYY-MM-DD format' });
    }

    try {
      const result = await runQuery(
        `
        WITH
          datetime($asOfDate) AS dayStart,
          datetime($asOfDate) + duration({days: 1}) AS dayEnd

        MATCH (a:Location {pipelineCode: $pipelineCode, locationId: $fromLocationId})
        MATCH (b:Location {pipelineCode: $pipelineCode, locationId: $toLocationId})
        MATCH p = shortestPath((a)-[:CONNECTS_TO*..200]->(b))

        UNWIND range(0, size(relationships(p)) - 1) AS i
        WITH
          i,
          nodes(p)[i]     AS fromLoc,
          nodes(p)[i + 1] AS toLoc,
          relationships(p)[i] AS seg,
          dayStart, dayEnd

        OPTIONAL MATCH (fromLoc)-[:HAS_CONSTRAINT]->(cf:Constraint)
        WHERE cf.effectiveDatetime < dayEnd AND cf.endDatetime >= dayStart
        WITH
          i, fromLoc, toLoc, seg, dayStart, dayEnd,
          count(cf) > 0 AS fromHasConstraint

        OPTIONAL MATCH (toLoc)-[:HAS_CONSTRAINT]->(ct:Constraint)
        WHERE ct.effectiveDatetime < dayEnd AND ct.endDatetime >= dayStart
        WITH
          i, fromLoc, toLoc, seg, fromHasConstraint,
          count(ct) > 0 AS toHasConstraint

        RETURN
          i + 1 AS seq,
          {
            locationId: fromLoc.locationId,
            name:   fromLoc.name,
            zone:   fromLoc.zone,
            type:   fromLoc.type,
            area:   fromLoc.area,
            direction: fromLoc.direction,
            position: fromLoc.position,
            hasConstraint: fromHasConstraint
          } AS fromLocation,
          {
            locationId: toLoc.locationId,
            name:   toLoc.name,
            zone:   toLoc.zone,
            type:   toLoc.type,
            area:   toLoc.area,
            direction: toLoc.direction,
            position: toLoc.position,
            hasConstraint: toHasConstraint
          } AS toLocation,
          seg.version       AS segmentVersion
        ORDER BY seq
        `,
        { pipelineCode, fromLocationId, toLocationId, asOfDate }
      );

      const rawSegments = result.records.map(r => {
        const obj = {};
        for (const key of r.keys) obj[key] = toPlain(r.get(key));
        return obj;
      });

    // finalize segments by enriching with capacity info
    const segments = await mapWithConcurrency(
      rawSegments,
      5, // keep modest — this doubles database calls
      async (s) => {
        const fromLocationId = s?.fromLocation?.locationId;
        const toLocationId   = s?.toLocation?.locationId;

        const [fromResult, toResult] = await Promise.all([
          fromLocationId
            ? getCapacityAndUtilizationAtLocation(pipelineCode, fromLocationId, 'RPQ', asOfDate, 1)
            : Promise.resolve({ capacity: [] }),
          toLocationId
            ? getCapacityAndUtilizationAtLocation(pipelineCode, toLocationId, 'DPQ', asOfDate, 1)
            : Promise.resolve({ capacity: [] })
        ]);

        const fromCapacity = fromResult.capacity?.[0] ?? null;
        const toCapacity   = toResult.capacity?.[0] ?? null;

        return {
          ...s,
          fromLocation: {
            ...s.fromLocation,
            capacity: fromCapacity
          },
          toLocation: {
            ...s.toLocation,
            capacity: toCapacity
          }
        };
      }
    );

    return res.json({
        params: { pipelineCode, fromLocationId, toLocationId, asOfDate },
        count: segments.length,
        segments
      });
    } catch (e) {
      console.error('path-details error:', e);
      return res.status(500).json({ error: e.message });
    }
  }
);

// GET /api/v1/pipelines/:pipelineCode/nominations/:flowDate  — fetch all nominations on a pipeline for a given flow date (YYYY-MM-DD)
// Example: /api/v1/pipelines/ANR/nominations/2025-11-01
app.get('/api/v1/pipelines/:pipelineCode/nominations/:flowDate(\\d{4}-\\d{2}-\\d{2})', async (req, res) => {
  const pipelineCode = req.params.pipelineCode;
  const flowDate = req.params.flowDate;

  try {
    const result = await runQuery(
      `
      WITH date($flowDate) AS d
      WITH d,
        datetime({date: d}) AS dayStart,
        datetime({date: d}) + duration('P1D') - duration('PT1S') AS dayEnd

      MATCH (rcpt:Location)-[n:NOMINATED]->(dlv:Location)
      WHERE n.pipelineCode = $pipelineCode AND n.flowDate = d

      CALL {
        WITH rcpt, dlv, dayStart, dayEnd
        MATCH p = allShortestPaths( (rcpt)-[:CONNECTS_TO*]->(dlv) )
        UNWIND nodes(p) AS loc
        OPTIONAL MATCH (loc)-[:HAS_CONSTRAINT]->(c:Constraint)
          WHERE c.effectiveDatetime <= dayEnd AND c.endDatetime >= dayStart
        WITH collect(DISTINCT {loc: loc, c: c}) AS raw
        RETURN [x IN raw WHERE x.c IS NOT NULL] AS hits   // [] if none
      }

      RETURN
        n.nomId          AS nomId,
        n.pipelineCode   AS pipelineCode,
        n.contractId     AS contractId,
        n.flowDate       AS flowDate,
        n.cycle          AS cycle,
        rcpt        AS receiptLocation,
        n.receiptVolume  AS receiptVolume,
        n.fuelLoss       AS fuelLoss,
        dlv         AS deliveryLocation,
        n.deliveryVolume AS deliveryVolume,
        [h IN hits | {
          locationName: h.loc.name,
          locationPipeline: h.loc.pipelineCode,
          constraintStart: h.c.effectiveDatetime,
          constraintEnd: h.c.endDatetime,
          constraintPercent: h.c.percent
        }] AS impactedLocations
      ORDER BY n.pipelineCode, n.nomId
      `,
      { pipelineCode, flowDate }
    );

    // Map records to plain JS objects
    const nominations = result.records.map(r => {
      const obj = {};
      for (const key of r.keys) {
        obj[key] = toPlain(r.get(key));
      }
      return obj;
    });

    res.json({ pipelineCode, flowDate, count: nominations.length, nominations });
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
        MATCH p = allShortestPaths( (rcpt)-[:CONNECTS_TO*]->(dlv) )
        UNWIND nodes(p) AS loc
        MATCH (loc)-[:HAS_CONSTRAINT]->(c:Constraint)
        WHERE c.effectiveDatetime <= dayEnd AND c.endDatetime >= dayStart   // time overlap
        RETURN collect(DISTINCT {loc: loc, c: c}) AS hits
      }

      WITH n, rcpt, dlv, hits
      WHERE size(hits) > 0
      RETURN
        n.nomId        AS nomId,
        n.pipelineCode AS pipelineCode,
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
          locationPipeline: h.loc.pipelineCode,
          constraintStart: h.c.effectiveDatetime,
          constraintEnd: h.c.endDatetime,
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
        MATCH p = allShortestPaths( (rcpt)-[:CONNECTS_TO*]->(dlv) )
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
      WHERE c.effectiveDatetime <= dayEnd AND c.endDatetime >= dayStart

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
        c.effectiveDatetime AS constraintStart,
        c.endDatetime       AS constraintEnd
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

// GET /api/v1/pipelines/:pipelineCode/agreements/firm-transport — fetch firm transport for a pipeline and date
// Example: /api/v1/pipelines/ANR/agreements/firm-transport?asOfDate=2025-11-01
app.get('/api/v1/pipelines/:pipelineCode/agreements/firm-transport', async (req, res) => {
  const pipelineCode = req.params.pipelineCode;
  const asOfDate = req.query.asOfDate;
  const shipperName = req.query.shipperName; // optional
  const limit = parseInt(req.query.limit) || 1000;
  const skip = parseInt(req.query.skip) || 0;

  try {
    const result = await runQuery(
      `
      MATCH (tc:TransportationContract {pipelineCode: $pipelineCode})
      MATCH (tc)-[:HAS_PERIOD]->(cp:ContractPeriod)
      WHERE tc.rateSchedule STARTS WITH 'FT'  // only firm transport
        ${asOfDate ? 'AND tc.effectiveDate <= date($asOfDate) <= tc.endDate AND cp.effectiveFrom <=  date($asOfDate) <= cp.effectiveTo' : ''}
        ${shipperName ? 'AND tc.shipperName = $shipperName' : ''}
        
      MATCH (cp)-[:PRIMARY_RECEIPT]->(rec:Location)
      MATCH (cp)-[:PRIMARY_DELIVERY]->(del:Location)
      RETURN
        tc.pipelineCode       AS pipelineCode,
        tc.shipperName        AS shipperName,
        tc.rateSchedule       AS rateSchedule,
        tc.contractId         AS contractId,
        tc.effectiveDate      AS contractEffectiveDate,
        tc.endDate            AS contractEndDate,
        tc.baseMDQ            AS baseMDQ,
        tc.flowUnit           AS flowUnit,
        cp.periodCode         AS periodCode,
        cp.effectiveFrom      AS periodEffectiveFrom,
        cp.effectiveTo        AS periodEffectiveTo,
        cp.mdq                AS mdq,
        rec.name              AS primaryReceipt,
        rec.locationId        AS primaryReceiptLocationId,
        del.name              AS primaryDelivery,
        del.locationId        AS primaryDeliveryLocationId
      ORDER BY tc.shipperName, tc.rateSchedule, tc.contractId SKIP toInteger($skip) LIMIT toInteger($limit);
      `,
      { pipelineCode, shipperName, asOfDate, limit, skip }
    );

    // Map records to plain JS objects
    const contracts = result.records.map(r => {
      const obj = {};
      for (const key of r.keys) {
        obj[key] = toPlain(r.get(key));
      }
      return obj;
    });

    res.json({ params: { pipelineCode, asOfDate }, count: contracts.length, contracts });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/v1/pipelines/:pipelineCode/capacities/operationally-available — fetch operationally available capacity for a pipeline and date
// Example: /api/v1/pipelines/ANR/capacities/operationally-available?asOfDate=2025-11-01
app.get('/api/v1/pipelines/:pipelineCode/capacities/operationally-available', async (req, res) => {
  const pipelineCode = req.params.pipelineCode;
  const asOfDate = req.query.asOfDate; // optional
  const cycle = req.query.cycle; // optional
  const locationId = req.query.locationId; // optional
  const limit = parseInt(req.query.limit) || 1000;
  const skip = parseInt(req.query.skip) || 0;

  try {
    const result = await runQuery(
      `
      MATCH (n:OperationallyAvailableCapacity) 
      WHERE n.pipelineCode = $pipelineCode
        ${asOfDate ? ' AND n.flowDate =  date($asOfDate)' : ''}
        ${cycle ? ' AND n.cycle = $cycle' : ''}
        ${locationId ? ' AND n.locationId = $locationId' : ''}
 
        RETURN
          n.pipelineCode                    AS pipelineCode,
          n.cycle                           AS cycle,
          n.designCapacity                  AS designCapacity,
          n.direction                       AS direction,
          n.flowDate                        AS flowDate,
          n.flowInd                         AS flowIndicator,
          n.grossOrNet                      AS grossOrNet,
          n.ITIndicator                     AS ITIndicator,
          n.locationName                    AS locationName,
          n.locationId                      AS locationId,
          n.locPurpDesc                     AS locPurpDesc,
          n.locQTI                          AS locQTI,
          n.schedStatus                     AS schedStatus,
          n.operatingCapacity               AS operatingCapacity,
          n.operationallyAvailableCapacity  AS operationallyAvailableCapacity,
          n.postingDate                     AS postingDatetime,
          n.totalSchedQty                   AS totalSchedQty
      ORDER BY n.pipelineCode, n.locationId, n.flowDate, n.cycle, n.postingDate DESC SKIP toInteger($skip) LIMIT toInteger($limit);
      `,
      { pipelineCode, asOfDate, cycle, locationId, limit, skip }
    );

    // Map records to plain JS objects
    const operationallyAvailableCapacity = result.records.map(r => {
      const obj = {};
      for (const key of r.keys) {
        obj[key] = toPlain(r.get(key));
      }
      return obj;
    });

    res.json({ params: { pipelineCode, asOfDate, cycle, locationId }, count: operationallyAvailableCapacity.length, operationallyAvailableCapacity, 
      page: { skip: Number(skip), limit: Number(limit) }});
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// todo: clean up these temporary validation helpers and move into a capacityValidator.js file
const mustStr = (v, field, rowIdx) => {
  // this just ensures the field is present as a string data type. A validator should check allowed values separately.
  if (v === undefined || v === null) {
    throw new ValidationError(`Row ${rowIdx}: Missing required field: ${field}`);
  }
  return String(v).trim();
};

const mustNum = (v, field, rowIdx) => {
  if (v === undefined || v === null || String(v).trim() === '') {
    throw new ValidationError(`Row ${rowIdx}: Missing required field: ${field}`);
  }
  const n = Number(v);
  if (!Number.isFinite(n)) throw new ValidationError(`Row ${rowIdx}: ${field} must be a number`);
  return n;
};

// PUT /api/v1/pipelines/:pipelineCode/capacities/operationally-available — upsert operationally available capacity records
// Body: { records: [ { pipelineCode, cycle, designCapacity, direction, flowDate, flowInd, grossOrNet, ITIndicator, locationName, locationId, locPurpDesc, locQTI, schedStatus, operatingCapacity, operationallyAvailableCapacity, postingDate, totalSchedQty }, ... ] }
app.put('/api/v1/pipelines/:pipelineCode/capacities/operationally-available', async (req, res) => {
  const pipelineCode = req.params.pipelineCode;
  const body = req.body ?? {};

  // Accept: { records:[...] } OR { ...singleRecord }
  const incomingRows = Array.isArray(body.records) ? body.records : [body];

  if (incomingRows.length === 0) {
    return res.status(400).json({ error: 'No records provided' });
  }
  if (incomingRows.length > 200) {
    return res.status(400).json({ error: 'Too many records provided; submit batches with fewer than 200 records' });
  }

  // Normalize + validate
  let rows;
  try {
    rows = incomingRows.map((r, idx) => ({
      pipelineCode,
      cycle: mustStr(r.cycle, 'cycle', idx).toUpperCase(),
      flowDate: mustStr(r.flowDate, 'flowDate', idx),
      postingDate: mustStr(r.postingDate, 'postingDate', idx),

      locationId: mustStr(r.locationId, 'locationId', idx),
      locationName: mustStr(r.locationName, 'locationName', idx),

      locPurpDesc: mustStr(r.locPurpDesc, 'locPurpDesc', idx),
      locQTI: mustStr(r.locQTI, 'locQTI', idx).toUpperCase(),
      direction: mustStr(r.direction, 'direction', idx),
      flowIndicator: mustStr(r.flowIndicator, 'flowIndicator', idx).toUpperCase(),
      grossOrNet: mustStr(r.grossOrNet, 'grossOrNet', idx).toUpperCase(),
      schedStatus: mustStr(r.schedStatus, 'schedStatus', idx).toUpperCase(),

      designCapacity: mustNum(r.designCapacity, 'designCapacity', idx),
      operatingCapacity: mustNum(r.operatingCapacity, 'operatingCapacity', idx),
      operationallyAvailableCapacity: mustNum(r.operationallyAvailableCapacity, 'operationallyAvailableCapacity', idx),
      totalSchedQty: mustNum(r.totalSchedQty, 'totalSchedQty', idx),

      itIndicator: mustStr(r.itIndicator, 'itIndicator', idx).toUpperCase()
    }));
  } catch (e) {
    return res.status(400).json({ error: e.message });
  }

  try {
    validateOacBatch(rows);
  } catch (e) {
    if (e instanceof ValidationError) {
      return res.status(400).json({ error: e.message, details: e.details });
    }
    return res.status(400).json({ error: e.message });
  }

  // Cypher query to upsert batch of OAC records
  const cypherUpsertOacBatch = `
    UNWIND $rows AS row
    WITH
      row,
      datetime(row.postingDate) AS incomingPosting,
      toUpper(row.schedStatus)   AS schedStatus,
      toUpper(row.cycle)         AS cycle,
      toUpper(row.locQTI)        AS locQTI,
      toUpper(row.direction)     AS direction,
      toUpper(row.flowIndicator) AS flowIndicator,
      toUpper(row.grossOrNet)    AS grossOrNet,
      toUpper(row.itIndicator)   AS itIndicator

    MERGE (oac:OperationallyAvailableCapacity {
      pipelineCode:  row.pipelineCode,
      cycle:         cycle,
      flowDate:      date(row.flowDate),
      locationId:    row.locationId,
      locPurpDesc:   row.locPurpDesc,
      locQTI:        locQTI,
      direction:     direction,
      flowIndicator: flowIndicator,
      grossOrNet:    grossOrNet,
      schedStatus:   schedStatus
    })
    ON CREATE SET
      oac.createdAt = datetime(),
      oac.postingDate = incomingPosting

    WITH row, oac, incomingPosting, itIndicator,
        CASE
          WHEN oac.postingDate IS NULL THEN true
          WHEN incomingPosting >= oac.postingDate THEN true
          ELSE false
        END AS shouldUpdate

    FOREACH (_ IN CASE WHEN shouldUpdate THEN [1] ELSE [] END |
      SET
        oac.postingDate = incomingPosting,
        oac.locationName = row.locationName,
        oac.designCapacity = toInteger(row.designCapacity),
        oac.operatingCapacity = toInteger(row.operatingCapacity),
        oac.operationallyAvailableCapacity = toInteger(row.operationallyAvailableCapacity),
        oac.totalSchedQty = toInteger(row.totalSchedQty),
        oac.itIndicator = itIndicator,
        oac.updatedAt = datetime()
    )

    WITH row, oac, shouldUpdate
    OPTIONAL MATCH (l:Location)
    WHERE l.pipelineCode = row.pipelineCode
      AND (
        l.locationId = row.locationId
        OR l.name = row.locationName
      )

    FOREACH (_ IN CASE WHEN l IS NULL OR NOT shouldUpdate THEN [] ELSE [1] END |
      MERGE (l)-[:HAS_AVAILABLE_CAPACITY]->(oac)
    )

    RETURN
      count(*) AS received,
      sum(CASE WHEN shouldUpdate THEN 1 ELSE 0 END) AS applied,
      sum(CASE WHEN shouldUpdate THEN 0 ELSE 1 END) AS ignored;
  `;

  // Single write query call
  const neo = await runQuery(cypherUpsertOacBatch, { rows }, 'WRITE');

  if (!neo.records || neo.records.length === 0) {
    throw new Error('OAC upsert returned no results');
  }

  // Cypher returns exactly one record with scalar fields
  const rec = neo.records[0];

  const response = {
    pipelineCode,
    counts: {
      received: toPlain(rec.get('received')),
      applied:  toPlain(rec.get('applied')),
      ignored:  toPlain(rec.get('ignored'))
    }
  };

  // 200 OK for PUT
  return res.status(200).json(response);
});

// GET /api/v1/pipelines/:pipelineCode/capacities/cycles-with-data — fetch cycles that have capacity data for a pipeline and date
// Example: /api/v1/pipelines/ANR/capacities/cycles-with-data?asOfDate=2026-01-22
app.get('/api/v1/pipelines/:pipelineCode/capacities/cycles-with-data', async (req, res) => {
  const pipelineCode = req.params.pipelineCode;
  const asOfDate = req.query.asOfDate; // required

  if (!asOfDate) {
    return res.status(400).json({ error: 'asOfDate query parameter is required' });
  }

  try {
    const result = await runQuery(
      `
      MATCH (n:OperationallyAvailableCapacity)
      WHERE n.pipelineCode = $pipelineCode
        AND n.flowDate = date($asOfDate)

      MATCH (c:Cycle)
      WHERE c.pipelineCode = n.pipelineCode
        AND c.cycleCode = n.cycle

      RETURN DISTINCT
        n.flowDate AS flowDate,
        n.cycle    AS cycle,
        c.sortOrder AS sortOrder
      ORDER BY c.sortOrder;
      `,
      { pipelineCode, asOfDate }
    );

    // Map records to plain JS objects
    const cycles = result.records.map(r => toPlain(r.toObject()));

    res.json({ params: { pipelineCode, asOfDate }, count: cycles.length, cycles });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/v1/pipelines/:pipelineCode/flows/:startDate/:endDate?limit=100&skip=0  — fetch meter volumes for a pipeline and date range w/ pagination
// Example: /api/v1/pipelines/ANR/flows/2025-11-01/2025-11-30?limit=50&skip=0
app.get('/api/v1/pipelines/:pipelineCode/flows/:startDate(\\d{4}-\\d{2}-\\d{2})/:endDate(\\d{4}-\\d{2}-\\d{2})', async (req, res) => {
  const pipelineCode = req.params.pipelineCode;
  const startDate = req.params.startDate;
  const endDate = req.params.endDate;
  const limit = parseInt(req.query.limit) || 100;
  const skip  = parseInt(req.query.skip)  || 0;
  const locationId = req.query.locationId;  // optional
  const cycle = req.query.cycle; // optional

  try {
    const result = await runQuery(
      `
      MATCH (o:OperationalFlow)
      WHERE o.pipelineCode = $pipelineCode AND o.flowDate >= date($startDate) AND o.flowDate <= date($endDate)
        ${locationId ? 'AND o.locationId = $locationId' : ''}
        ${cycle ? 'AND o.cycle = $cycle' : ''}
      RETURN
        o.pipelineCode          AS pipelineCode,
        o.locationId            AS locationId,
        o.flowDate              AS flowDate,
        o.cycle                 AS cycle,
        o.operationalCapacity   AS operationalCapacity,
        o.scheduledVolume       AS scheduledVolume,
        o.utilization           AS utilizationPerCent
      ORDER BY o.pipelineCode, o.locationId, o.flowDate, o.cycle SKIP toInteger($skip) LIMIT toInteger($limit)
      `,
      { pipelineCode, startDate, endDate, limit, skip, locationId, cycle }
    );

    // Map records to plain JS objects
    const flows = result.records.map(r => {
      const obj = {};
      for (const key of r.keys) {
        obj[key] = toPlain(r.get(key));
      }
      return obj;
    });

    res.json({ params: { pipelineCode, startDate, endDate, locationId, cycle }, count: flows.length, flows, page: { skip: Number(skip), limit: Number(limit) } });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/v1/pipelines/:pipelineCode/prices/:startDate/:endDate?limit=100&skip=0  — fetch prices for a pipeline and date range w/ pagination
// Example: /api/v1/pipelines/ANR/prices/2025-11-01/2025-11-30?limit=50&skip=0
app.get('/api/v1/pipelines/:pipelineCode/prices/:startDate(\\d{4}-\\d{2}-\\d{2})/:endDate(\\d{4}-\\d{2}-\\d{2})', async (req, res) => {
  const pipelineCode = req.params.pipelineCode;
  const startDate = req.params.startDate;
  const endDate = req.params.endDate;
  const limit = parseInt(req.query.limit) || 100;
  const skip  = parseInt(req.query.skip)  || 0;

  try {
    const result = await runQuery(
      `
      MATCH (r:Region)-[hs:HAS_SYMBOL]->(s:Symbol)-[htd:HAS_TRADING_DAY]->(td:SymbolTradingDay)
      WHERE r.pipelineCode = $pipelineCode AND td.date >= datetime($startDate) AND td.date <= datetime($endDate)
      RETURN
        r as region,
        s as symbol,
        td AS symbolTradingDay
      ORDER BY r.name, s.code, td.date, td.modificationDate DESC SKIP toInteger($skip) LIMIT toInteger($limit)
      `,
      { pipelineCode, startDate, endDate, limit, skip }
    );

    // Map records to plain JS objects
    const prices = result.records.map(r => {
      const obj = {};
      for (const key of r.keys) {
        obj[key] = toPlain(r.get(key));
      }
      return obj;
    });

    res.json({ params: { pipelineCode, startDate, endDate }, count: prices.length, prices, page: { skip: Number(skip), limit: Number(limit) } });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

//PUT /api/v1/prices/symbol-trading-day — upsert prices
// Body: { prices: [ { symbol, tradingDay, high?, low?, mid?, close?, volume?, modificationDatetime? }, ... ] }
app.put('/api/v1/prices/symbol-trading-day', async (req, res) => {
  const body = req.body ?? {};
  const incomingPrices = Array.isArray(body.prices) ? body.prices : [body];

  if (incomingPrices.length === 0) {
    return res.status(400).json({ error: 'No price records provided' });
  }
  if (incomingPrices.length > 200) {
    return res.status(400).json({ error: 'Too many price records provided; submit batches with fewer than 200 records' });
  }

  try {
    const result = await runQuery(
      `
      UNWIND $rows AS row
      WITH
        row,
        toUpper(trim(row.symbol)) AS symbol,
        date(substring(toString(row.tradingDay), 0, 10)) AS d,
        CASE
          WHEN row.modificationDatetime IS NULL OR trim(toString(row.modificationDatetime)) = '' THEN NULL
          ELSE datetime(toString(row.modificationDatetime))
        END AS modDt

      // Require Symbol to exist
      OPTIONAL MATCH (s:Symbol {code: symbol})
      WITH row, symbol, d, modDt, s

      CALL (row, symbol, d, modDt, s) {
        // If symbol missing, emit a failure result, do not write anything
        WITH row, symbol, d, modDt, s
        WHERE s IS NULL
        RETURN {
          symbol: symbol,
          date: toString(d),
          outcome: 'FAILED_UNKNOWN_SYMBOL'
        } AS r

        UNION

        WITH row, symbol, d, modDt, s
        WHERE s IS NOT NULL

        MERGE (td:SymbolTradingDay {symbol: symbol, date: d})
        ON CREATE SET
          td.ID = symbol + "_" + toString(d),
          td.createdAt = datetime(),
          td._justCreated = true
        SET
          td.high   = CASE WHEN row.high   IS NULL OR trim(toString(row.high))   = '' THEN NULL ELSE toFloat(row.high) END,
          td.low    = CASE WHEN row.low    IS NULL OR trim(toString(row.low))    = '' THEN NULL ELSE toFloat(row.low) END,
          td.mid    = CASE WHEN row.mid    IS NULL OR trim(toString(row.mid))    = '' THEN NULL ELSE toFloat(row.mid) END,
          td.close  = CASE WHEN row.close  IS NULL OR trim(toString(row.close))  = '' THEN NULL ELSE toFloat(row.close) END,
          td.volume = CASE WHEN row.volume IS NULL OR trim(toString(row.volume)) = '' THEN NULL ELSE toInteger(row.volume) END,
          td.modificationDate = modDt,
          td.updatedAt = datetime()

        WITH s, td, coalesce(td._justCreated,false) AS isNew
        FOREACH (_ IN CASE WHEN isNew THEN [1] ELSE [] END |
          MERGE (s)-[:HAS_TRADING_DAY]->(td)
        )
        REMOVE td._justCreated

        RETURN {
          symbol: td.symbol,
          date: toString(td.date),
          id: td.ID,
          outcome: CASE WHEN isNew THEN 'CREATED' ELSE 'UPDATED' END
        } AS r
      }

      WITH collect(r) AS results
      RETURN
        results,
        {
          received: size(results),
          created:  size([x IN results WHERE x.outcome = 'CREATED']),
          updated:  size([x IN results WHERE x.outcome = 'UPDATED']),
          failedUnknownSymbol: size([x IN results WHERE x.outcome = 'FAILED_UNKNOWN_SYMBOL'])
        } AS counts;

      `,
      { rows: incomingPrices },
      'WRITE'
    );

    const counts = result.records[0].get('counts');
    res.json({ received: toPlain(counts.received), created: toPlain(counts.created),
       updated: toPlain(counts.updated), failedUnknownSymbol: toPlain(counts.failedUnknownSymbol) });
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
  `MATCH p = (a:Location {name: $from})-[:CONNECTS_TO${hopPattern}]->(b:Location {name: $to})
   WITH p
   UNWIND nodes(p) AS loc
   OPTIONAL MATCH (loc)-[:HAS_CONSTRAINT]->(c:Constraint)
   WHERE c.effectiveDatetime <= datetime($at) AND (c.endDatetime IS NULL OR c.endDatetime >= datetime($at))
   WITH p, collect(DISTINCT loc {.*, id: id(loc), constrained: count(c) > 0}) AS locs,
        [rel IN relationships(p) | rel {.*, id: id(rel), type: type(rel)}] AS rels
   RETURN locs AS nodes, rels AS relationships
  ` :
  `MATCH p = (a:Location {name: $from})-[:CONNECTS_TO${hopPattern}]->(b:Location {name: $to})
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

// GET /api/v1/pipelines/:pipelineCode/notices?noticeType=Type&asOf=DateTime&limit=50&skip=0
// Get notices for a pipeline; if asOf provided, filters on effectiveDate and endDate.
app.get('/api/v1/pipelines/:pipelineCode/notices', async (req, res) => {
  const pipelineCode = req.params.pipelineCode;
  const { category, noticeType, startDate, endDate, limit = 100, skip = 0 } = req.query;
  
  // startDate and endDate are required
  if (!startDate || !endDate) {
    return res.status(400).json({ error: 'startDate and endDate are required query params' });
  }


  const baseMatch = `MATCH (n:Notice) WHERE n.pipelineCode = $pipelineCode`;

  const timeFilter = `
    AND n.effectiveDatetime <= datetime($endDate)
    AND (n.endDatetime IS NULL OR n.endDatetime >= datetime($startDate))
  `;

  const categoryFilter = category ? `AND n.category = $category` : '';
  const typeFilter = noticeType ? `AND n.noticeType = $noticeType` : '';

  const cypher = `
    ${baseMatch}
    ${timeFilter}
    ${categoryFilter}
    ${typeFilter}
    WITH n ORDER BY n.endDatetime DESC, n.effectiveDatetime DESC SKIP toInteger($skip) LIMIT toInteger($limit)
    RETURN
      n.pipelineCode        AS pipelineCode,
      n.noticeId            AS noticeId,
      n.postingDatetime     AS postingDatetime,
      n.noticeType          AS noticeType,
      n.category            AS category,
      n.status              AS status,
      n.subject             AS subject,
      n.priorNoticeId       AS priorNoticeId,
      n.lastModifiedDatetime AS lastModifiedDatetime,
      n.effectiveDatetime   AS effectiveDatetime,
      n.endDatetime         AS endDatetime,
      n.content             AS content
  `;

  try {
    const result = await runQuery(cypher, { pipelineCode, startDate, endDate, category, noticeType, limit: Number(limit), skip: Number(skip) });
    //const notices =
    //  result.records.length > 0
    //    ? toPlain(result.records[0].get('notices'))
    //    : [];

    // Map records to plain JS objects
    const notices = result.records.map(r => {
      const obj = {};
      for (const key of r.keys) {
        obj[key] = toPlain(r.get(key));
      }
      return obj;
    });

    res.json({ pipelineCode, startDate, endDate, category, noticeType, count: notices.length, notices, page: { skip: Number(skip), limit: Number(limit) } });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/v1/pipelines/:pipelineCode/notices/:noticeId
// Get a single notice for a pipeline
app.get('/api/v1/pipelines/:pipelineCode/notices/:noticeId', async (req, res) => {
  const pipelineCode = req.params.pipelineCode;
  const noticeId = req.params.noticeId;

  const cypher = `
    MATCH (n:Notice {pipelineCode: $pipelineCode, noticeId: $noticeId})

    CALL (n) {
      OPTIONAL MATCH (n)-[:CREATES_CONSTRAINT]->(c:Constraint)
      OPTIONAL MATCH (l:Location)-[:HAS_CONSTRAINT]->(c)
      WITH
        c,
        collect(DISTINCT CASE WHEN l IS NULL THEN NULL ELSE l {
          .pipelineCode,
          .locationId,
          .name,
          .type,
          .direction,
          .zone,
          .marketArea,
          .state,
          .county,
          .pipelineSegmentCode,
          .position
        } END) AS locs
      WITH
        c,
        [x IN locs WHERE x IS NOT NULL] AS locations
      RETURN collect(
        CASE WHEN c IS NULL THEN NULL ELSE {
          pipelineCode:      c.pipelineCode,
          effectiveDatetime: c.effectiveDatetime,
          endDatetime:       c.endDatetime,
          limit:             c.limit,
          flowUnit:          c.flowUnit,
          source:            c.source,
          createdAt:         c.createdAt,
          kind:              c.kind,
          locations:         locations
        } END
      ) AS constraints
    }

    RETURN n { .* , constraints: [x IN constraints WHERE x IS NOT NULL] } AS notice;
  `;

  try {
    const result = await runQuery(cypher, { pipelineCode, noticeId });
    if (result.records.length === 0) {
      return res.status(404).json({
        error: 'Notice not found',
        pipelineCode,
        noticeId
      });
    }

    // There is exactly one row and one column: "notice"
    const notice = toPlain(result.records[0].get('notice'));

    res.json({ pipelineCode, noticeId, notice });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/v1/pipelines/:pipelineCode/constraints?location=Name&asOf=ISO&limit=50&skip=0
// If location omitted, returns constraints across graph; if asOf provided, filters active asOf time.
app.get('/api/v1/pipelines/:pipelineCode/constraints', async (req, res) => {
  const pipelineCode = req.params.pipelineCode;
  const { location, asOf, limit = 100, skip = 0 } = req.query;
  const atTime = asOf ? new Date(asOf).toISOString() : null;

  const baseMatch = location ?
    `MATCH (l:Location {name: $location})-[:HAS_CONSTRAINT]->(c:Constraint)` :
    `MATCH (c:Constraint)`;

  const where = 'WHERE c.pipelineCode = $pipelineCode';

  const timeFilter = atTime ?
    `  AND c.effectiveDatetime <= datetime($asOf) AND (c.endDatetime IS NULL OR c.endDatetime >= datetime($asOf))` : '';

  const cypher = `
    ${baseMatch}
    ${where}
    ${timeFilter}
    WITH c ORDER BY c.effectiveDatetime DESC SKIP toInteger($skip) LIMIT toInteger($limit)
    RETURN collect(c {.*, id: id(c)}) AS constraints
  `;

  try {
    const result = await runQuery(cypher, { pipelineCode, location, asOf: atTime, limit: Number(limit), skip: Number(skip) });
    const constraints =
      result.records.length > 0
        ? toPlain(result.records[0].get('constraints'))
        : [];

    res.json({ pipelineCode, location, asOf, count: constraints.length, constraints, page: { skip: Number(skip), limit: Number(limit) } });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/v1/notices/:pipeline  — create a new Notice on a pipeline
// Body: { noticeId, category, noticeType, content, effectiveDatetime, endDatetime?, postingDatetime, lastModifiedDatetime?, subject, priorNoticeId? }
app.post('/api/v1/notices/:pipeline', async (req, res) => {
  const pipelineCode = req.params.pipeline;
  const body = req.body ?? {};
  
  // Accept either: { ...notice } OR { notices: [ ... ] }
  const notices = Array.isArray(body.notices) ? body.notices : [body];

  if (notices.length === 0) {
    return res.status(400).json({ error: 'No notices provided' });
  }

  // Required fields
  const required = [
    'noticeId',
    'category',
    'noticeType',
    'content',
    'effectiveDatetime',
    'postingDatetime',
    'status',
    'subject'
  ];
  // endDatetime, lastModifiedDatetime, and priorNoticeId are optional

  const normalize = (n) => {
    for (const k of required) {
      if (n[k] === undefined || n[k] === null || String(n[k]).trim() === '') {
        throw new Error(`Missing required field: ${k}`);
      }
    }
    return {
      noticeId: String(n.noticeId).trim(),
      category: String(n.category).trim(),
      noticeType: String(n.noticeType).trim(),
      content: String(n.content),
      effectiveDatetime: String(n.effectiveDatetime).trim(),
      endDatetime: (n.endDatetime === undefined || n.endDatetime === null || String(n.endDatetime).trim() === '')
        ? null
        : String(n.endDatetime).trim(),
      postingDatetime: String(n.postingDatetime).trim(),
      lastModifiedDatetime: (n.lastModifiedDatetime === undefined || n.lastModifiedDatetime === null || String(n.lastModifiedDatetime).trim() === '')
        ? n.postingDatetime
        : String(n.lastModifiedDatetime).trim(),
      status: String(n.status).trim(),
      subject: String(n.subject).trim(),
      priorNoticeId: (n.priorNoticeId === undefined || n.priorNoticeId === null || String(n.priorNoticeId).trim() === '')
        ? null
        : String(n.priorNoticeId).trim()
    };
  };

  let normalized;
  try {
    normalized = notices.map(normalize);
  } catch (e) {
    return res.status(400).json({ error: e.message });
  }

  const cypher = `
    UNWIND $notices AS notice
    WITH notice, datetime(notice.lastModifiedDatetime) AS incomingMod
    MERGE (n:Notice { pipelineCode: $pipelineCode, noticeId: notice.noticeId })
    ON CREATE SET
      n.createdAt = datetime()

    WITH n, notice, incomingMod,
        CASE
          WHEN n.lastModifiedDatetime IS NULL THEN true
          WHEN incomingMod >= n.lastModifiedDatetime THEN true
          ELSE false
        END AS shouldUpdate

    FOREACH (_ IN CASE WHEN shouldUpdate THEN [1] ELSE [] END |
      SET
        n.category = notice.category,
        n.noticeType = notice.noticeType,
        n.content = notice.content,
        n.effectiveDatetime = datetime(notice.effectiveDatetime),
        n.endDatetime = CASE WHEN notice.endDatetime IS NULL THEN NULL ELSE datetime(notice.endDatetime) END,
        n.postingDatetime = datetime(notice.postingDatetime),
        n.status = notice.status,
        n.subject = notice.subject,
        n.priorNoticeId = notice.priorNoticeId,
        n.lastModifiedDatetime = incomingMod,
        n.updatedAt = datetime()
    )
    RETURN n;
  `;

  try {
    const result = await runQuery(cypher, { pipelineCode, notices: normalized }, 'WRITE');
    const created = result.records.map(r => toPlain(r.get('n').properties));
    return res.status(201).json({ pipelineCode, count: created.length, notices: created });
  } catch (e) {
    if (String(e.code || '').includes('ConstraintValidationFailed')) {
      return res.status(409).json({ error: 'One or more notices already exist (constraint violation)' });
    }
    return res.status(500).json({ error: e.message });
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

// Example helper function to get scheduled quantity for a contract as of a date
// In real implementation, this would query the database or another data source like Endur.
async function getScheduledQty(contractId, asOfDate) {
  if (contractId == "123966") {
    return 200000;
  }
  else if (contractId == "125082") {
    return 82000;
  }
  return 300000;
}

// Example helper function to get maximum capacity for a contract as of a date
// In real implementation, this would query the database and do some calculations that are still tbd.
// It's also likely that max capacity would depend on more than just contractId and asOfDate,
// so the entire contractObj is passed as a parameter. That way, additional properties can be used as needed.
async function calculateMaxCapacity(contractObj, asOfDate) {
  if (contractObj.constraints && contractObj.constraints.length === 0) {
    return contractObj.mdq; // if no constraints, max capacity = mdq
  }
  else if (contractObj.contractId == "123966") {
    return 160000;
  }
  return 235000;
}

// GET /volumes/capacity-and-utilization/:pipeline/:locationId/:locQTI/:asOfDate
// Example: /volumes/capacity-and-utilization/ANR/312115/DPQ/2025-11-01
app.get(
  '/volumes/capacity-and-utilization/:pipeline/:locationId/:locQTI/:asOfDate(\\d{4}-\\d{2}-\\d{2})',
  async (req, res) => {
    const { pipeline, locationId, locQTI, asOfDate } = req.params;

    try {
      const data = await getCapacityAndUtilizationAtLocation(
        pipeline,
        locationId,
        locQTI,
        asOfDate
      );

      res.json(data);
    } catch (e) {
      console.error('capacity-and-utilization error:', e);
      res.status(500).json({ error: e.message });
    }
  }
);

// Helper function to get capacity and utilization at a location
async function getCapacityAndUtilizationAtLocation(
  pipelineCode,
  locationId,
  locQTI,
  asOfDate,
  limit = 100 // default limit far exceeds the number of expected results
) {
  const locationNumberStr =
    typeof locationId === 'string'
      ? locationId
      : locationId.toString();

  const result = await runQuery(
    `
    MATCH (l:Location)
    WHERE l.pipelineCode = $pipelineCode
      AND l.locationId = $locationId

    MATCH (l)-[:HAS_AVAILABLE_CAPACITY]->(n:OperationallyAvailableCapacity)
    WHERE n.locQTI = $locQTI
      AND n.flowDate = date($asOfDate)

    RETURN
      n.pipelineCode                    AS pipelineCode,
      n.locationId                      AS locationId,
      n.locationName                    AS locationName,
      n.flowDate                        AS flowDate,
      n.cycle                           AS cycle,
      n.designCapacity                  AS designCapacity,
      n.operatingCapacity               AS operatingCapacity,
      n.totalSchedQty                   AS totalSchedQty,
      n.operationallyAvailableCapacity  AS operationallyAvailableCapacity,
      CASE
        WHEN n.operatingCapacity IS NULL OR n.operatingCapacity = 0 THEN NULL
        ELSE 100.0 * n.operationallyAvailableCapacity / n.operatingCapacity
      END                               AS availablePercent,
      CASE
        WHEN n.operatingCapacity IS NULL OR n.operatingCapacity = 0 THEN NULL
        ELSE 100.0 * n.totalSchedQty / n.operatingCapacity
      END                               AS utilizationPercent,
      n.schedStatus                     AS schedStatus,
      n.locQTI                          AS locQTI,
      n.locPurpDesc                     AS locPurpDesc,
      n.itIndicator                     AS itIndicator,
      n.grossOrNet                      AS grossOrNet,
      n.flowIndicator                   AS flowIndicator,
      n.direction                       AS direction,
      n.postingDate                     AS postingDatetime
    ORDER BY n.postingDate DESC
    LIMIT toInteger($limit);
    `,
    { pipelineCode, locationId, locQTI, asOfDate, limit }
  );


  const capacity = result.records.map(r => {
    const obj = {};
    for (const key of r.keys) {
      obj[key] = toPlain(r.get(key));
    }
    return obj;
  });

  return {
    params: { pipelineCode, locationId: locationId, locQTI, asOfDate },
    count: capacity.length,
    capacity
  };
}

// Helper function to fetch valid Zone names for a pipeline 
// todo: cache results to avoid repeated queries
async function fetchZoneNamesForPipeline(runQuery, pipelineCode) {
  const cypher = `
    MATCH (z:Zone {pipelineCode: $pipelineCode})
    RETURN collect(z.name) AS names
  `;
  const r = await runQuery(cypher, { pipelineCode });
  return new Set(r.records[0].get('names'));
}

// Helper function to fetch valid LocationType codes 
// todo: cache results to avoid repeated queries
async function fetchLocationTypeCodesForPipeline(runQuery) {
  const cypher = `
    MATCH (lt:LocationType)
    RETURN collect(lt.code) AS codes
  `;
  const r = await runQuery(cypher);
  return new Set(r.records[0].get('codes'));
}

// Helper function to map over items with a concurrency limit to avoid overwhelming the database
async function mapWithConcurrency(items, limit, mapper) {
  const results = new Array(items.length);
  let i = 0;

  async function worker() {
    while (true) {
      const idx = i++;
      if (idx >= items.length) return;
      results[idx] = await mapper(items[idx], idx);
    }
  }

  const workers = Array.from(
    { length: Math.min(limit, items.length) },
    () => worker()
  );

  await Promise.all(workers);
  return results;
}

// ---- Start server ----
app.listen(PORT, () => {
  console.log(`Neo4j API listening on http://localhost:${PORT}`);
});

// Graceful shutdown
process.on('SIGINT', async () => {
  await driver.close();
  process.exit(0);
});
