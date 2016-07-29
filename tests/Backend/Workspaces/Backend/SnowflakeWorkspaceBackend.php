<?php

namespace Keboola\Test\Backend\Workspaces\Backend;

use Keboola\Db\Import\Snowflake\Connection;

class SnowflakeWorkspaceBackend implements WorkspaceBackend {

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

    public function countRows($table)
    {
        $count = $this->db->fetchAll(sprintf("SELECT count(*) FROM %s;", $this->db->quoteIdentifier($table)));
        var_dump($count);
        return $count[0]['COUNT(*)'];
    }

    public function toIdentifier($item) {
        return $item;
    }
}