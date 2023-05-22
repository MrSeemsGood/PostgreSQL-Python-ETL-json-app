# PyQt5 app for JSON parsing and ETL processing
This app is a fancy PyQt5 wrapper for JSON parsing and performing ETL process on a data, inserting it into PostgreSQL database afterwards.

.json files have fixed structure:
```json
{
  "process_id": "number",
  "data": [
    {
      "id": "number",
      "price_change": [
        {
          "price": "number",
          "eff_from": "date"
        },
        ...
      ]
    },
    ...
  ]
} 
```
Data is then stored as:
| id | price | eff_from | eff_to|
| --- | --- | --- | --- |
| int | int | date | date |

, where `eff_to` is calculated automatically as the last `eff_from` value of this `id` (-1 day) if it exists, or "5999-12-31" if it does not.

Database preparation
---
The ETL process itself is supported by the following trigger function inside of the database:
```sql
CREATE OR REPLACE FUNCTION prices_schema.prices_etl() RETURNS TRIGGER AS $$
BEGIN
    UPDATE prices_schema.prices p0
    SET eff_to = subquery.next_eff_from
    FROM (
        SELECT id, price, eff_from, COALESCE(
            LEAD(eff_from, 1)
            OVER (
                PARTITION BY id
                ORDER BY eff_from ASC
            ),
            TO_DATE('6000-00-00', 'YYYY-MM-DD')
        ) - 1 AS next_eff_from FROM prices_schema.prices
    ) AS subquery
	WHERE subquery.id = p0.id
	AND subquery.price = p0.price
	AND subquery.eff_from = p0.eff_from;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER after_insert_prices
AFTER INSERT ON prices_schema.prices
FOR EACH ROW EXECUTE PROCEDURE prices_schema.prices_etl();
```

Testing
---
Inserting `test_json1.json` via the app produces the following table, as expected:
| id | price | eff_from | eff_to|
| --- | --- | --- | --- |
| 1	| 100	| "2017-01-12"	| "2017-02-08"
| 1	| 150	| "2017-02-09"	| "2017-09-22"
| 1	| 100	| "2017-09-23"	| "5999-12-31"
| 2	| 10	| "2016-09-10"	| "2017-06-05"
| 2	| 15	| "2017-06-06"	| "5999-12-31"
