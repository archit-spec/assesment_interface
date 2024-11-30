apis

first upload the data
upload:
```
curl -X POST -F "file=@Payment Report Sheet - Hiring - Sheet1.csv" http://localhost:8000/upload/Payment
curl -X POST -F file=@"Merchant Tax Report (MTR) Sheet - Hiring.xlsx" http://localhost:8000/api/upload/mtr
```

processed : curl "http://localhost:8000/processed?page=1&size=10"

unprocessed : curl "http://localhost:8000/unprocessed?page=1&size=10"

## API Query Examples
```
GET /unprocessed?page=1&size=10
GET /processed?page=1&size=10
GET /processed/{id}
GET /summary?start_date=2023-01-01&end_date=2023-12-31
GET /transactions?page=1&size=50
GET /transaction/{order_id}
GET /status/{job_id}

```
