# esqlj-elastic-jdbc
A JDBC driver for Elastic fully open source.

esqlj not use at all SQL Elastic implementation, Elastic integration is built on top of Elastic Rest High Level API

## Project status
Beta stage. 

SQL Layers: implemented DQL, not implemented DDL and DML

## Driver class

### org.fpasti.jdbc.esqlj.EsDriver

## JDBC Connections string

JDBC url must to be follow this syntax:

```
jdbc:esqlj:http<s>://<elastic_address_1>:<elastic_port_1>,http://<elastic_address_2>:<elastic_port_2>,...;param1=paramValue1;...
```
It's possible to declare a pool of connections listing the url of Elastic instances:

Optional parameters:

| Name | Description | Default value
|--- |--- |---
| userName | Credential user name | -
| password | Credential password | -
| includeTextFieldsByDefault | Include text typed fields by default on select * | false
| indexMetaDataCache | Cache retrieved index structure. Select execution engine requires to know index / alias structure, retrieve this information it could be an heavy operation especially for alias or starred index query. Best choice to enable it on unmutable index | true
| queryScrollFromRows | Number of rows fetched on first pagination | 500
| queryScrollFetchSize | Fetched rows on next pagination | 500
| queryScrollTimeoutMinutes | Timeout between pagination expressed in minutes | 3
| queryScrollOnlyByScrollApi | If false apply the scroll strategy that best fit the query (see Pagination paragraph below) | true
| sharedConnection | If true rest client will be statically shared between all connection (use it if you don't have the requirement to connect to different Elastic clusters | true


## Concepts

Elastic indices are managed like SQL Tables.  
Elastic aliases are managed like SQL Views. 

Query on index / alias containing special character like '*', '-', '.' need to be double quoted. For example 'SELECT * FROM ".test-index*"'  
Field and alias containing special characters like '-' must also to be double quoted.

Document identifier "_id" is returned like a column and mapped on MetaData like primary key. This column is also available on Where condition for matching query (=, !=)

'Like' filter is implemented by Wildcard Elastic Query (https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html)

About SQL implementation see below section 'Support matrix and conventions'

By default the maximum number of document fields that can be retrieved is set to 100. This explain for example because .kibana_* index containing almost 500 fields return an error on 'select *'. For increasing this configuration threshold change this Elastic setting according to your needs: index.max_docvalue_fields_search

Change behaviour on indices that start with 'my-index':
PUT /my-index*/_settings
{
  "index" : {
    "index.max_docvalue_fields_search" : 500
  }
}

Change behaviour on all indices:
PUT /*/_settings
{
  "index" : {
    "index.max_docvalue_fields_search" : 500
  }
}

In the future there will no longer be possible query system index.

## DBeaver



## Types

Mapping of Elastic types to SQL types:

| Elastic on index type | Metadata declared SQL Type | Java effective type 
|--- |--- |---
| binary | TINYINT | Byte
| boolean | BOOL | Boolean
| date  | TIMESTAMP | LocalDateTime
| date_nanos | TIMESTAMP | LocalDateTime
| doc_id | VARCHAR | String
| double | NUMBER | Double
| flattened | STRUCT | Object
| float | NUMBER | Float
| geo_point | STRUCT | EsGeoPoint
| half_float | NUMBER | Float
| integer | NUMBER | Integer
| ip | VARCHAR | String
| keyword | VARCHAR | String
| long | BIGINT | Long
| object | STRUCT | Object
| scaled_float | NUMBER | Float
| short | NUMBER | Byte
| text | VARCHAR | String
| unsigned_long | NUMBER | Long
| wildcard | VARCHAR | String

## Pagination

By default esqlj implements a scrolling strategy on query through Elastic Scroll API. Optionally it's possibile to activate the less expensive scroll by order, but if you want to activate this functionality pay attention to include in every query a sorting on at least one tiebreaker field (in future it's no longer possible to query by doc id, it could be a best practice to store identifier also in document field).  It's in discussion an RFC on Elastic product about the introduction of an automatic tiebreaker in query result. But for now if you enable this feature and miss to add a sorting on a tiebreaker fields some rows could be skipped between paginations of data.

Still on the subject of scrolling by order, the driver will automatically try to use the Point in time API if Elastic 7.10 is detected. (It's required to use the linked compiled JAR for using this feature because Rest high level API seems don't implements it for now..)

Pay attention: Scroll API consume resources on server. It's a best practice to fetch all required data as soon as possible. The scroll link will be automatically released from esql at the end of data retrieve.

## Testing
Most of test units require a live Elastic instance.  
The activation of these units is commanded by a system variable named "ESQLJ_TEST_CONFIG".  
The environment variabile must concatenate a valid esqlj JDBC url connection string and the load strategy of the documents requested by query inside units:

```
ESQLJ_TEST_CONFIG="jdbc:esqlj:http://<elastic_address>:<elastic_port>|<createAndDestroy or createOnly>
```

| Parameters | Actions | Scope
|--- |--- |---
| createAndDestroy | Test units create index 'esqlj-test-volatile-\<uuid\>' on start and delete it on finish | Continuous Delivery/Deployment
| createOnly | Test units create index 'esqlj-test-static-\<release.version\>' and not delete it on finish. If it's just present on ElasticSearch it will be preserved. (Will be required a manual delete of it from system).| Development stage

Sample configuration:
ESQLJ_TEST_CONFIG="jdbc:esqlj:http://10.77.154.32:9080|createOnly"

If ESQLJ_TEST_CONFIG isn't declared, all tests depending from live connection will be skipped. 

## Support matrix and conventions

### From clause
Supported: column, alias, *

### Where condition

The column must to be declared typically on left of expression (`value`=column is managed like invalid from esqlj)  
You can use both column name or column alias in expression.

| Expression condition | Notes
|--- |--- 
| `column` = `value` | 
| `column` != `value` | 
| `column` > `numeric_value` | 
| `column` >= `numeric_value` | 
| `column` < `numeric_value` | 
| `column` <= `numeric_value` | 
| `column` LIKE `expression` | Implemented by Wildcard ElasticSearch filter. See ElasticSearch documentation about its usage
| `column` IS NULL |
| `column` IS NOT NULL |
| `column` BETWEEN `a` AND `b` | `a` and `b` could be NUMBER, STRING, date expressed by TO_DATE('date', 'mask_date'), EXTRACT function

#### Functions

| Function name | Notes
|--- |--- 
| SYSDATE | Current date time
| SYSDATE() | Current date time
| NOW() | Current date time
| GETDATE() | Current date time
| TRUNC(SYSDATE\|SYSDATE()) | Current date
| TO_DATE(`date`, `mask_date`) | Supported mask: YEAR, YYYY, YY, MM, MONTH, MON, DDD, DD, HH24, HH12, HH, MI, SS, DAY, XFF, FFF, FF, F, PM, TZR, TZH. Example TO_DATE('2020/01/01', 'YYYY/MM/DD')
| EXTRACT(`PERIOD` FROM `column`) | PERIOD can be valued with `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`. Usage example: EXTRACT(YEAR FROM timestamp)!=2020

### Order

Example:
SELECT * FROM `column` ORDER BY keywordField, integerField DESC

### Limit

Example:
SELECT * FROM `column` LIMIT 100


## Compatibility

Tested on 7.4.2 and 7.10.0 ElasticSearch release

## About me
Fabrizio Pasti
fabrizio.pasti@gmail.com
www.linkedin.com/in/fabrizio-pasti-2340a627

