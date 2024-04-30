package table

import (
  "database/sql"
  "log"

  _ "github.com/lib/pq"
  "github.com/pkg/errors"
)

type (
  // Columns describes a resource which we grant API access to.
  Column struct {
    number            string
    name              string
    col_description   string
    is_notnull        string
    table_description string
    field_type        string
    is_primary_key    string
    is_unique_key     string
    is_foreign_key    string
    default_value     string
  }
)

// GetData returns the columns from the table with the given name.
func GetData(db *sql.DB, tableName string) (*sql.Rows, error) {
  rows, err := db.Query(`SELECT  
    f.attnum AS number,  
    f.attname AS name,
    col_description('core.interaction_messages'::regClass, f.attnum) col_description,
    CASE  
        WHEN f.attnotnull=TRUE THEN 'Y'
        ELSE 'N'  
    END AS is_notnull,
    obj_description(c.oid) table_description,
    pg_catalog.format_type(f.atttypid,f.atttypmod) AS field_type,  
    CASE  
        WHEN p.contype = 'p' THEN 'Y'  
        ELSE 'N'  
    END AS is_primary_key,  
    CASE  
        WHEN p.contype = 'u' THEN 'Y'  
        ELSE 'N'
    END AS is_unique_key,
    CASE
        WHEN p.contype = 'f' THEN 'Y'
        else 'N'
    END AS is_foreign_key,
    CASE
        WHEN f.atthasdef = 't' THEN pg_get_expr(d.adbin, d.adrelid)
    END AS default_value
FROM pg_attribute f  
    JOIN pg_class c ON c.oid = f.attrelid  
    JOIN pg_type t ON t.oid = f.atttypid  
    LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum  
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace  
    LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND f.attnum = ANY (p.conkey)  
    LEFT JOIN pg_class AS g ON p.confrelid = g.oid  
WHERE c.relkind = 'r'::char  
    AND n.nspname = 'core'  -- Replace with Schema name  
    AND c.relname = 'interaction_types'  -- Replace with table name  
    AND f.attnum > 0 ORDER BY number
`)
  if err != nil {
    return nil, errors.WithStack(err)
  }
  defer rows.Close()

  j2 := make(jValutes)
  for rows.Next() {
    var clmn Column
    var sT1 sql.NullString

    if err = rows.Scan(&jVls.Id, &jVls.Date, &jVls.NumCode, &jVls.Nominal, &jVls.Value,
      &jVls.ValueUnit, &jVls.LoadDate, &jVls.Digits, &jVls.EnglishName, &sT1); err != nil {
      log.Fatal(err)
    }
    if sT1.Valid {
      jVls.LocalName = sT1.String
    }
    jVls.CharCode = jVls.Id
    j2[jVls.Id] = jVls
  }
  if err := rows.Err(); err != nil {
    log.Fatal(err)
  }
  log.Print(Rows)

  return rows, nil
}
