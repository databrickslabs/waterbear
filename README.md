![](images/lakehouse-for-financial-services.jpeg)

Following the launch of the [Lakehouse for Financial Services](https://databricks.com/solutions/industries/financial-services), 
we created that project to convert data models expressed as JSON schema into spark Schema and Delta Live Tables expectations.

## Usage

```python
from databricks.lh4fs import L4FSModel
model = L4FSModel('/path/to/json/models').load("employee")
schema = model.schema
constraints = model.constraints
```

```python
@dlt.create_table()
def bronze():
  return (
    spark
      .readStream
      .format(file_format)  # we read standard data sources
      .schema(model.schema) # ... but enforce schema
      .load('/path/to/data/files')
  )
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

```python
@dlt.create_table()
@dlt.expect_all_or_drop(model.constraints)
def silver():
  return dlt.read_stream("bronze")
```

![](images/pipeline_processing.png)

## Project support
Please note that all projects in the /databrickslabs github account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support.

## Authors
<antoine.amend@databricks.com>