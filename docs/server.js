const express = require('express')

const app = express()
const version = "1.0"
app.use(express.static('./build/site'))
app.get('/', (req, res) => res.redirect('rdflib-neo4j/' + version))
app.listen(8000, () => console.log('📘 http://localhost:8000'))