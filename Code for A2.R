install.packages("RPostgres")
library(RPostgres)

wrds <- dbConnect(
  Postgres(),
  host = 'wrds-pgdata.wharton.upenn.edu',
  port = 9737,
  dbname = 'wrds',
  user = 'harrisonvu',
  password = 'Suangontuyetvoi!2311',
  sslmode = 'require'
)


wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user='harrisonvu')


qry <- "
SELECT gvkey, conm, fyear, at, che, lt, capx, xrd, sale,
oancf, ib, dp, cogs, invt
FROM comp.funda
WHERE indfmt = 'INDL'
AND datafmt = 'STD'
AND popsrc = 'D'
AND consol = 'C'
AND fyear BETWEEN 2020 AND 2025
"
# 1. Send query
res <- dbSendQuery(wrds, qry)

# 2. Fetch all rows (-1 means all)
funda <- dbFetch(res, n = -1)

# 3. Clear result
dbClearResult(res)

# 4. Inspect data
head(funda)












