const express = require('express');
const { Pool } = require('pg');
const { createClient } = require('redis');

const app = express();
app.use(express.json());

// Configuration
const PORT = process.env.PORT || 3000;

// PostgreSQL Configuration
// Writes go to HAProxy (which points to Primary)
const writePool = new Pool({
  user: 'app',
  host: 'localhost', // HAProxy mapped port
  database: 'appdb',
  password: 'app_pwd',
  port: 5439, 
});

// Reads go to Replica directly
const readPool = new Pool({
  user: 'app',
  host: 'localhost', // Replica mapped port
  database: 'appdb',
  password: 'app_pwd',
  port: 5433,
});

// Redis Configuration
let redisConfig;

if (process.env.REDIS_SENTINELS) {
  // Format: "host:port,host:port"
  const sentinels = process.env.REDIS_SENTINELS.split(',').map(s => {
    const [host, port] = s.split(':');
    return { host, port: parseInt(port) };
  });
  
  redisConfig = {
    socket: {
      sentinel: {
        nodes: sentinels,
        name: process.env.REDIS_MASTER_NAME || 'mymaster'
      }
    }
  };
  console.log('Using Redis Sentinel Configuration');
} else {
  redisConfig = {
    url: process.env.REDIS_URL || 'redis://localhost:6379'
  };
}

const redisClient = createClient(redisConfig);

redisClient.on('error', (err) => console.log('Redis Client Error', err));

// Anti Cache-Stampede: Request Coalescing Map
const pendingRequests = new Map();

async function startServer() {
  await redisClient.connect();

  console.log('Connected to Redis');

  app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
  });
}

// Routes placeholders
app.get('/', (req, res) => {
  res.send('TP BDD Caching API');
});

// GET /products/:id - Cache-aside with Request Coalescing (Anti-Stampede)
app.get('/products/:id', async (req, res) => {
  const { id } = req.params;
  const cacheKey = `product:${id}`;

  try {
    // 1. Check Redis
    const cachedData = await redisClient.get(cacheKey);
    if (cachedData) {
      console.log(`Cache HIT for ${cacheKey}`);
      return res.json(JSON.parse(cachedData));
    }

    console.log(`Cache MISS for ${cacheKey}`);

    // 2. Request Coalescing: Check if a fetch is already in progress
    if (pendingRequests.has(cacheKey)) {
      console.log(`Request Coalescing: Waiting for pending fetch for ${cacheKey}`);
      const product = await pendingRequests.get(cacheKey);
      return res.json(product);
    }

    // 3. Fetch from DB (wrapped in a promise stored in the map)
    const fetchPromise = (async () => {
      try {
        const result = await readPool.query('SELECT * FROM products WHERE id = $1', [id]);
        if (result.rows.length === 0) return null;
        
        const product = result.rows[0];
        
        // Write to Redis (TTL 60s)
        await redisClient.set(cacheKey, JSON.stringify(product), { EX: 60 });
        
        return product;
      } finally {
        // Remove from pending map when done (success or failure)
        pendingRequests.delete(cacheKey);
      }
    })();

    pendingRequests.set(cacheKey, fetchPromise);

    const product = await fetchPromise;

    if (!product) {
      return res.status(404).json({ error: 'Product not found' });
    }

    res.json(product);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// POST /products - Write to Primary
app.post('/products', async (req, res) => {
  const { name, price_cents } = req.body;
  try {
    const result = await writePool.query(
      'INSERT INTO products(name, price_cents) VALUES($1, $2) RETURNING *',
      [name, price_cents]
    );
    res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// PUT /products/:id - Write to Primary + Invalidate Cache
app.put('/products/:id', async (req, res) => {
  const { id } = req.params;
  const { name, price_cents } = req.body;
  const cacheKey = `product:${id}`;

  try {
    // 1. Update Primary
    const result = await writePool.query(
      'UPDATE products SET name = $1, price_cents = $2, updated_at = NOW() WHERE id = $3 RETURNING *',
      [name, price_cents, id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Product not found' });
    }

    // 2. Invalidate Cache
    await redisClient.del(cacheKey);
    console.log(`Cache INVALIDATED for ${cacheKey}`);

    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});


startServer();
