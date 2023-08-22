const express = require('express');
const jwt = require('jsonwebtoken');
const app = express();
const dotenv = require('dotenv');
const cors = require('cors');
const { MongoClient } = require('mongodb');
dotenv.config();
app.use(express.json());
app.use(cors());


const uri = process.env.MONGO_DB_URI;
const SECRET_KEY = process.env.SECRET_KEY;
const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });
const db = client.db('Bookstore');
let collection;

async function createCollection() {
  try {
    const collectionName = 'books';
    const collections = await db.collections();
    const collectionExists = collections.some((collection) => collection.collectionName === collectionName);
    if (!collectionExists) {
      const validator = {
        $jsonSchema: {
          bsonType: 'object',
          required: ['title', 'authors'],
          properties: {
            title: {
              bsonType: 'string',
              description: 'Type must be a string and it is required',
            },
            authors: {
              bsonType: 'array',
              items: {
                bsonType: 'string',
              },
              description: 'Type must be an array of strings and it is required',
            },
            description: {
              bsonType: 'string',
              description: 'Type must be a string (optional)',
            },
            publication_year: {
              bsonType: 'int',
              minimum: 1800, // minimum acceptable year
              maximum: new Date().getFullYear(), // maximum acceptable year
              description: 'Publication year should be within this range (optional)',
            },
          },
        },
      };
      const collectionResponse = await db.createCollection(collectionName, { validator });
      // Create a text index on the "title", "authors" and "description" fields
      await collectionResponse.createIndex(
        { title: "text", authors: "text", description: "text" }
      );
    } else {
      console.log('Collection already exists:', collectionName);
    }
    collection = db.collection(collectionName);
  } catch (err) {
    console.error('Error creating collection:', err);
  }
}

async function checkMongoDBConnection() {
  try {
    await client.connect();
    console.log('Connected to MongoDB');
    createCollection();
  } catch (err) {
    console.error('Error connecting to MongoDB:', err);
  }
}

checkMongoDBConnection();

// verify jwt token for authentication
function authVerifyMiddleware (req) {
  const token = req.header('Authorization');
  if (!token) {
      return { status: 404, message: 'Authentication failed. Token not found.' };
  }
  try {
      // You can store the authenticated user's data in the request object for later use
      const decoded = jwt.verify(token, SECRET_KEY);
      return { token: decoded, status: 200, message: 'Authentication successful.' };
  } catch (err) {
      return { status: 401, message: 'Token gets expired' };
  }
};

// create new token for authentication
function createAuthToken(role) {
  return jwt.sign({
            role,
         }, SECRET_KEY, { expiresIn: '12h' });
}

// get books by offset, limit and sort order
app.get('/books', async (req, res) => {
  let { offset, limit, sort, order } = req.query;
  try {
    const response = authVerifyMiddleware(req);
    if (response.status === 200) {
      // Calculate the offset based on the requested page number and perPage count
      offset = offset > 1 ? (parseInt(offset, 10) - 1) * limit : 0;
      limit = limit > 0 ? parseInt(limit, 10) : 10;
      let options = {};
      let validFields = ['title', 'authors', 'description', 'publication_year'];
      order = order ? parseInt(order, 10) : 1;
      if(validFields.includes(sort)) {
        if(sort === 'authors' ) {
            sort = 'authors.0';
        }
        options = { [sort]:  order };
      } else {
        res.status(403).json({ error: 'Invalid sort fields' });
      }
      if (order !== -1 && order !== 1) {
        res.status(403).json({ error: 'Invalid order format' });
      }
      // Fetch records with pagination using limit() and skip()
      const data = await collection.find({}).sort(options).skip(offset).limit(limit).toArray();
      res.status(200).json({ message: data.length ? 'Successfully Fetched' : 'There is no documents available', books: data });
    } else {
      res.status(401).json(response);
    }
  } catch (err) {
    console.error('Error fetching books:', err);
    res.status(404).json({ error: 'Not found' });
  }
});

// fetch data using id
app.get('/books/:id', async (req, res) => {
  const id = parseInt(req.params.id, 10);
  try {
    const response = authVerifyMiddleware(req);
    if (response.status === 200) {
      if(req.params.id || req.params.id === 0) {
        const data = await collection.findOne({ _id: id });
        res.status(200).json({ message: data ? 'Document fetched successfully' : 'There is no document available', books: data });
      } else {
        res.status(403).json({ error: 'Invalid id' });
      }
    } else {
      res.status(401).json(response);
    }
  } catch (err) {
    console.error('Error fetching books:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// fuzzy search using title | authors | description
app.get('/books/search', async (req, res) => {
  const { query } = req.query;
  try {
    const response = authVerifyMiddleware(req);
    if (response.status === 200) {
      const data = await collection.find({ $text: { $search: query ? query : '' } }).toArray();
      console.log('data', data);
      res.status(200).json({ message: data ? 'Document fetched successfully' : 'There is no document available', books: data });
    } else {
      res.status(401).json(response);
    }
  } catch (err) {
    console.error('Error fetching books:', err);
    res.status(404).json({ error: 'Not found' });
  }
});

// insert single book
app.post('/books', async (req, res) => {
  const { title, authors, description, publication_year } = req.body;
  const sortedAuthors = authors.sort();
  try {
    const response = authVerifyMiddleware(req);
    let count = 0;
    if (response.status === 200) {
      if (title && authors) {
        const pipeline = [
          { $sort: { _id: -1 } },
          { $limit: 1 }
        ];
        const result = await collection.aggregate(pipeline).toArray();
        if(result.length) {
          count = result[0]._id + 1;
        }
        const newDocument = {
          _id: count, 
          title, 
          authors: sortedAuthors,
        };
        if(description) {
          newDocument.description = description;
        }
        if(publication_year) {
          newDocument.publication_year = publication_year;
        }
        const book =  await collection.insertOne(newDocument);
        res.status(200).json({ message: 'Successfully inserted', book});
      } else {
        res.status(403).json({ error: 'Both title and authors fields are required' });
      }
    } else {
      res.status(401).json(response);
    }
  } catch (err) {
    console.error('Error inserting book:', err);
    res.status(403).json({ error: 'Invalid data format' });
  }
});

// fetch data using id
app.put('/books/:id', async (req, res) => {
  const id = parseInt(req.params.id, 10);
  const updateEntry = { $set: req.body };
  const filter = { _id: id };
  const options = { upsert: true };
  try {
    const response = authVerifyMiddleware(req);
    if (response.status === 200) {
      if (req.body.title && req.body.authors) {
        if(req.params.id || req.params.id === 0) {
          const data = await collection.updateOne(filter, updateEntry, options);
          res.status(200).json({ message: 'Document upserted successfully', books: data});
        } else {
          res.status(403).json({ error: 'Invalid id' });
        }
      } else {
        res.status(403).json({ error: 'Both title and authors fields are required' });
      }
    } else {
      res.status(401).json(response);
    }
  } catch (err) {
    console.error('Error updating books:', err);
    res.status(500).json({ error: 'Interanl server error' });
  }
});

// fetch data using id
app.delete('/books/:id', async (req, res) => {
  const id = parseInt(req.params.id, 10);
  try {
    const response = authVerifyMiddleware(req);
    if (response.status === 200) {
      if(req.params.id || req.params.id === 0) {
        const result = await collection.deleteOne({ _id: id });
        if (result.deletedCount === 1) {
          res.status(200).json({ message: "Successfully deleted." });
        } else {
          res.status(404).json({ error: 'Not found' });
        }
      } else {
        res.status(403).json({ error: 'Invalid id' });
      }
    } else {
      res.status(401).json(response);
    }
  } catch (err) {
    console.error('Error deleting books:', err);
    res.status(500).json({ error: 'Interanl server error' });
  }
});

app.get('/createAuthToken', (req, res) => {
  let { role } = req.query;
  if(role) {
    const token = createAuthToken(role);
    res.status(200).json({ token, message: 'Token generated Successfully. It is valid for 12h.' });
  } else {
    res.status(404).json({ error: 'Not found' });
  }
});

// Close MongoDB connection when the server is stopped
function closeMongoDBConnection() {
  client.close()
    .then(() => console.log('MongoDB connection closed'))
    .catch((err) => console.error('Error closing MongoDB connection:', err));
}

// Event listeners for server shutdown signals
process.on('beforeExit', closeMongoDBConnection);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});