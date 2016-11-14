<?php

namespace Keboola\Test\Backend\Workspaces\Backend;

class RedshiftWorkspaceBackend implements WorkspaceBackend
{

    private $db;
    
    private $schema;

    private function getDbConnection($connection)
    {
        $pdo = new \PDO(
            "pgsql:dbname={$connection['database']};port=5439;host=" . $connection['host'],
            $connection['user'],
            $connection['password']
        );
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        return $pdo;
    }
    
    public function __construct($workspace)
    {
        $this->db = $this->getDbConnection($workspace['connection']);
        $this->schema = $workspace['connection']['schema'];
    }
    
    public function getTableColumns($table)
    {
        $stmt = $this->db->prepare("SELECT \"column\" FROM PG_TABLE_DEF WHERE tablename = ?;");
        $stmt->execute(array($table));
        return array_map(function ($row) {
                return $row['column'];
        }, $stmt->fetchAll());
    }

    public function getTables()
    {
        $stmt = $this->db->prepare("select tablename from PG_TABLES where schemaname = ?");
        $stmt->execute(array($this->schema));
        return array_map(function ($table) {
                return $table['tablename'];
        }, $stmt->fetchAll());
    }

    public function dropTable($table)
    {
        $this->db->query(sprintf("DROP TABLE \"{$this->schema}\".\"%s\";", $table));
    }

    public function dropTableColumn($table, $column)
    {
        $this->db->query(sprintf(
            "ALTER TABLE  \"{$this->schema}\".\"%s\" DROP COLUMN \"%s\";",
            $table,
            $column
        ));
    }

    public function countRows($table)
    {
        $stmt = $this->db->prepare(sprintf("select count(*) as count from \"{$this->schema}\".\"%s\"", $table));
        $stmt->execute();
        $count = $stmt->fetch();
        return $count['count'];
    }

    public function fetchAll($table, $style = \PDO::FETCH_NUM)
    {
        $stmt = $this->db->prepare(sprintf("SELECT * FROM \"{$this->schema}\".\"%s\"", $table));
        $stmt->execute();
        return $stmt->fetchAll($style);
    }

    public function createTable($tableName, $columns)
    {
        $cols = [];
        foreach ($columns as $column => $datatype) {
            $cols[] = "\"{$column}\" {$datatype}";
        }
        $definition = join(",\n", $cols);

        $this->db->query(
            sprintf("CREATE TABLE %s (%s)", $tableName, $definition)
        );
    }

    public function toIdentifier($item)
    {
        if (is_array($item)) {
            return array_map(function ($item) {
                return strtolower($item);
            }, $item);
        } else {
            return strtolower($item);
        }
    }

    public function describeTableColumns($tableName)
    {
        $sql = "SELECT
                a.attnum,
                n.nspname,
                c.relname,
                a.attname AS colname,
                t.typname AS type,
                a.atttypmod,
                FORMAT_TYPE(a.atttypid, a.atttypmod) AS complete_type,
                d.adsrc AS default_value,
                a.attnotnull AS notnull,
                a.attlen AS length,
                co.contype,
                ARRAY_TO_STRING(co.conkey, ',') AS conkey
            FROM pg_attribute AS a
                JOIN pg_class AS c ON a.attrelid = c.oid
                JOIN pg_namespace AS n ON c.relnamespace = n.oid
                JOIN pg_type AS t ON a.atttypid = t.oid
                LEFT OUTER JOIN pg_constraint AS co ON (co.conrelid = c.oid
                    AND a.attnum = ANY(co.conkey) AND co.contype = 'p')
                LEFT OUTER JOIN pg_attrdef AS d ON d.adrelid = c.oid AND d.adnum = a.attnum
            WHERE a.attnum > 0 AND c.relname = " . $this->db->quote($tableName);
        
        $sql .= " AND n.nspname = " . $this->db->quote($this->schema);
        
        $sql .= ' ORDER BY a.attnum';

        $stmt = $this->db->prepare($sql);

        $stmt->execute();

        $result = $stmt->fetchAll();

        $attnum = 0;
        $nspname = 1;
        $relname = 2;
        $colname = 3;
        $type = 4;
        $atttypemod = 5;
        $complete_type = 6;
        $default_value = 7;
        $notnull = 8;
        $length = 9;
        $contype = 10;
        $conkey = 11;

        $desc = [];
        foreach ($result as $key => $row) {
            $defaultValue = $row[$default_value];
            if ($row[$type] == 'varchar' || $row[$type] == 'bpchar') {
                if (preg_match('/character(?: varying)?(?:\((\d+)\))?/', $row[$complete_type], $matches)) {
                    if (isset($matches[1])) {
                        $row[$length] = $matches[1];
                    } else {
                        $row[$length] = null; // unlimited
                    }
                }
                if (preg_match("/^'(.*?)'::(?:character varying|bpchar)$/", $defaultValue, $matches)) {
                    $defaultValue = $matches[1];
                }
            }
            list($primary, $primaryPosition, $identity) = [false, null, false];
            if ($row[$contype] == 'p') {
                $primary = true;
                $primaryPosition = array_search($row[$attnum], explode(',', $row[$conkey])) + 1;
                $identity = (bool)(preg_match('/^nextval/', $row[$default_value]));
            }
            $desc[$row[$colname]] = [
                'SCHEMA_NAME' => $row[$nspname],
                'TABLE_NAME' => $row[$relname],
                'name' => $row[$colname],
                'COLUMN_POSITION' => $row[$attnum],
                'DATA_TYPE' => $row[$type],
                'DEFAULT' => $defaultValue,
                'NULLABLE' => (bool)($row[$notnull] != 't'),
                'LENGTH' => $row[$length],
                'SCALE' => null, // @todo
                'PRECISION' => null, // @todo
                'UNSIGNED' => null, // @todo
                'PRIMARY' => $primary,
                'PRIMARY_POSITION' => $primaryPosition,
                'IDENTITY' => $identity
            ];
        }
        return $desc;
    }
}
