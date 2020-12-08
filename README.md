# Getting the Data
## Daily Infections

The data used int this database was fetched via the OpenDataSUS ElasticSearch API. All credentials and endpoints needed for the API are already in the code.

Further info can be found at: [https://opendatasus.saude.gov.br/dataset/casos-nacionais](https://www.saopaulo.sp.gov.br/coronavirus/isolamento/)

The function `getCases` downloads all confirmed positive cases from the cities present in the file`municipios.json`  and saves it on the file `cases.json`.

## Social Distancing Rate
Data from the State of São Paulo website was also used. The Social Distancing Data was downloaded at [https://www.saopaulo.sp.gov.br/coronavirus/isolamento/](https://www.saopaulo.sp.gov.br/coronavirus/isolamento/). Search for the download button at the bottom right. Click it. Select option 'Crosstab' and CSV and download to this folder.

# Parsing the Data
## Daily Infections

`processCases` organizes data by city and removes unecessary info from the cases and saves it to `casesClean.json`
`processCasesClean` removes data with incorrect date and saves it to `casesCleanBounded.json`
## Social Distancing Rate
`transformTaxaIsolamento` transforms the downloaded CSV into JSON and saves it to `taxaIsolamento.json`
`processTaxaIsolamento` organizes data by city and saves it to `taxaIsolamentoParsed.json`
`removeNaN` replaces missing data by the average value and saves it to `taxaIsolamentoClean.json`

# Creating the database

`processCasesCleanBounded` joins all the data from the two sources and creates the database saving it to `database.json`

----

### Authors
-   Eduardo Amaral
-   Vivian Kiyomi Amano