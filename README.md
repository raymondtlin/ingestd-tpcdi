# ingestd

## The TPC-DI Benchmark

Created by the Transaction Processing Council as a benchmark for ETL applications, the TPC-DI benchmark involves integrating multiple data sources and file formats into a data warehouse schema.  The benchmark specifies requirements both for historical and incremental loading, as well as transformations to be applied.

### Dataset

As outlined in the official TPC documentation found [here](http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-di_v1.1.0.pdf), the data model for the benchmark is a retail brokerage firm.

Flat files originate from 5 primary sources:
1.  OLTP: CDC extracts, which are bar-delimited txt
2.  HR: comma delimited database extract
3.  Prospect List: comma delimited csv
4.  Financial Newswire: Variable Fixed Width Format
5.  CRM: XML

The destination DWH schema will have the following:

| Facts          | Dimensions   | Reference   |
|----------------|--------------|-------------|
| fCashBalances  | dCustomers   | TradeTypes  |
| fHoldings      | dAccounts    | StatusTypes |
| fWatches       | dBrokers     | TaxRates    |
| fMarketHistory | dSecurities  | Industries  |
| fProspects     | dCompanies   | Financials  |
| fTrade         | dDate, dTime |             |

### Kafka Ingestion & Integration

### Data Serialization, Versioning
