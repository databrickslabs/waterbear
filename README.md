![](images/lakehouse-for-financial-services.jpeg)

Following the launch of the [Lakehouse for Financial Services](https://databricks.com/solutions/industries/financial-services), 
we created that project to convert data models expressed as JSON schema into spark Schema and Delta Live Tables expectations.

## Usage

```python
from lh4fs.schema import JsonBuilder

schema, constraints = JsonBuilder('/path/to/json/models').build("employee")
```

### Retrieve schema and constraints

```json
{"metadata":{"desc":"Employee ID"},"name":"id","nullable":false,"type":"integer"}
{"metadata":{"desc":"Employee personal information"},"name":"person","nullable":false,"type":{"fields":[{"metadata":{"desc":"A person name, first or last"},"name":"first_name","nullable":true,"type":"string"},{"metadata":{"desc":"person last name"},"name":"last_name","nullable":true,"type":"string"},{"metadata":{"desc":"Person birth date"},"name":"birth_date","nullable":true,"type":"date"},{"metadata":{"desc":"employee nickname"},"name":"username","nullable":true,"type":"string"}],"type":"struct"}}
{"metadata":{"desc":"Employee first day of employment"},"name":"joined_date","nullable":true,"type":"date"}
{"metadata":{"desc":"Number of high fives"},"name":"high_fives","nullable":true,"type":"double"}
{"metadata":{"desc":"Employee skills"},"name":"skills","nullable":true,"type":{"containsNull":true,"elementType":"string","type":"array"}}
{"metadata":{"desc":"Employee role"},"name":"role","nullable":true,"type":"string"}
```

```json
{
  "[`high_fives`] VALUE": "`high_fives` IS NULL OR `high_fives` BETWEEN 1.0 AND 300.0",
  "[`id`] NULLABLE": "`id` IS NOT NULL",
  "[`person`.`username`] MATCH": "`person`.`username` IS NULL OR `person`.`username` RLIKE '^[a-z0-9]{2,}$'",
  "[`person`] NULLABLE": "`person` IS NOT NULL",
  "[`role`] VALUE": "`role` IS NULL OR `role` IN ('SA','CSE','SSA','RSA')",
  "[`skills`] SIZE": "`skills` IS NULL OR SIZE(`skills`) >= 1.0"
}
```

### Delta Live Tables

```python
@dlt.create_table()
def bronze():
    return (
        spark
            .readStream
            .format('XXX')  # we read standard data sources, json, csv, jdbc, etc.
            .schema(schema)  # ... but enforce schema
            .build('/path/to/data/files')
    )
```


```python
@dlt.create_table()
@dlt.expect_all_or_drop(constraints) # we enforce expectations and may drop record, ignore or fail pipelines
def silver():
  return dlt.read_stream("bronze")
```

![](images/pipeline_processing.png)

## Project support
Please note that all projects in the /databrickslabs github account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support.

## Authors
<antoine.amend@databricks.com>