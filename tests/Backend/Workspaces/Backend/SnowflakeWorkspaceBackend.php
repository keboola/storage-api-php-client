<?php

namespace Keboola\Test\Backend\Workspaces\Backend;

use Keboola\Db\Import\Snowflake\Connection;

class SnowflakeWorkspaceBackend implements WorkspaceBackend
{

    private $db;

    private $schema;

    private function getDbConnection($connection)
    {
        $db = new Connection([
            'host' => $connection['host'],
            'database' => $connection['database'],
            'warehouse' => $connection['warehouse'],
            'user' => $connection['user'],
            'password' => $connection['password'],
        ]);
        // set connection to use workspace schema
        $db->query(sprintf("USE SCHEMA %s;", $db->quoteIdentifier($connection['schema'])));

        return $db;
    }
    
    public function __construct($workspace)
    {
        $this->db = $this->getDbConnection($workspace['connection']);
        $this->schema = $workspace['connection']['schema'];
    }

    /**
     * @param $table
     * @return array of column names
     */
    public function getTableColumns($table)
    {
        return array_map(function ($column) {
            return $column['column_name'];
        }, $this->db->fetchAll(sprintf("SHOW COLUMNS IN %s", $this->db->quoteIdentifier($table))));
    }

    /**
     * @return array of table names
     */
    public function getTables()
    {
        return array_map(function ($table) {
            return $table['name'];
        }, $this->db->fetchAll(sprintf("SHOW TABLES IN SCHEMA %s", $this->db->quoteIdentifier($this->schema))));
    }

    public function dropTable($table)
    {
        $this->db->query(sprintf("DROP TABLE %s;", $this->db->quoteIdentifier($table)));
    }

    public function dropTableColumn($table, $column)
    {

        $this->db->query(sprintf(
            "ALTER TABLE %s DROP COLUMN %s;",
            $this->db->quoteIdentifier($table),
            $this->db->quoteIdentifier($column)
        ));
    }

    public function countRows($table)
    {
        $tables = $this->db->fetchAll(
            sprintf("SHOW TABLES LIKE '%s' IN SCHEMA %", $table, $this->schema)
        );
        return (int) $tables[0]['rows'];
    }

    public function createTable($tableName, $columns)
    {
        $cols = [];
        foreach ($columns as $column => $dataType) {
            $cols[] = $this->db->quoteIdentifier($column) . " " . $dataType;
        }
        $qry = sprintf("CREATE TABLE %s (%s)", $this->db->quoteIdentifier($tableName), implode(", ", $cols));
        $this->db->query($qry);
    }

    public function fetchAll($table, $style = \PDO::FETCH_NUM)
    {
        $data = array();
        $res = $this->db->fetchAll(sprintf(
            "SELECT * FROM %s.%s;",
            $this->db->quoteIdentifier($this->schema),
            $this->db->quoteIdentifier($table)
        ));
        switch ($style) {
            case \PDO::FETCH_NUM:
                foreach ($res as $row) {
                    $data[] = array_values($row);
                }
                break;
            case \PDO::FETCH_ASSOC:
                $data = $res;
                break;
            default:
                throw new \Exception("Unknown fetch style $style");
                break;
        }
        return $data;
    }

    public function toIdentifier($item)
    {
        return $item;
    }

    public function describeTableColumns($tableName)
    {
        return $this->db->fetchAll(sprintf('DESC TABLE %s.%s', $this->db->quoteIdentifier($this->schema), $this->db->quoteIdentifier($tableName)));
    }
}
