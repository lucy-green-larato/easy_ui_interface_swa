'use strict';

// Wrap Express in Azure Functions
const { createHandler } = require('azure-function-express');
const express = require('express');
const multer = require('multer');

// shared module (note: go UP one level from /api/ch-strategic/)
const chStrategic = require('../generate/kinds/ch-strategic');

// tiny Express app the function will delegate to
const app = express();
app.use(express.json({ limit: '2mb' }));
app.use(express.urlencoded({ extended: true }));

// OPTIONAL: strict allow-lists for future /api/pbi-export usage
const allowPbi = {
  // workspaceIds: ['xxxxxxxx-xxxx-....'],
  // reportIds: ['xxxxxxxx-xxxx-....'],
  // visualNames: ['VisualContainer1']
};

// Mount all ch-strategic endpoints from the shared module.
// IMPORTANT: pass the multer **module**, not a preconfigured instance.
chStrategic.mount(app, { multer, allowPbi });

// Export the Azure Function handler
module.exports = createHandler(app);
