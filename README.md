# Automated-Funding

To Run Locally:

First:
docker build -t automated-funding-api:local .

Second:
docker run `
  --env-file .env `
  -v C:\Users\dirk.hoffman\Documents\Automated-Funding\secrets:/secrets `
  -p 8000:8000 `
  automated-funding-api:local

$env:PORT=3000

Third:- form the front end directory -- always check the port
npm run dev 