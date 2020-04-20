<?php

namespace Keboola\Test\Backend\Workspaces\Backend;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Keboola\Datatype\Definition\Synapse;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\Schema\SynapseSchemaReflection;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;

class SynapseWorkspaceBackend implements WorkspaceBackend
{
    /** @var Connection */
    private $db;

    /** @var string */
    private $schema;

    /** @var AbstractPlatform */
    private $platform;

    public function __construct($workspace)
    {
        $this->db = $this->getDbConnection($workspace['connection']);
        $this->platform = $this->db->getDatabasePlatform();
        $this->schema = $workspace['connection']['schema'];
    }

    private function getDbConnection($connection)
    {
        return \Doctrine\DBAL\DriverManager::getConnection([
            'user' => $connection['user'],
            'password' => $connection['password'],
            'host' => $connection['host'],
            'dbname' => $connection['database'],
            'port' => 1433,
            'driver' => 'pdo_sqlsrv',
            'driverOptions' => [
                'ConnectRetryCount' => 5,
                'ConnectRetryInterval' => 10,
            ],
        ]);
    }

    /**
     * @param $table
     * @return array of column names
     */
    public function getTableColumns($table)
    {
        $ref = new SynapseTableReflection($this->db, $this->schema, $table);
        return $ref->getColumnsNames();
    }

    /**
     * @return array of table names
     */
    public function getTables()
    {
        $ref = new SynapseSchemaReflection($this->db, $this->schema);
        return $ref->getTablesNames();
    }

    public function dropTable($table)
    {
        $qb = new SynapseTableQueryBuilder($this->db);
        $this->db->exec($qb->getDropTableCommand($this->schema, $table));
    }

    public function dropTableColumn($table, $column)
    {
        $this->db->exec(sprintf(
            "ALTER TABLE %s.%s DROP COLUMN %s;",
            $this->platform->quoteSingleIdentifier($this->schema),
            $this->platform->quoteSingleIdentifier($table),
            $this->platform->quoteSingleIdentifier($column)
        ));
    }

    public function countRows($table)
    {
        $ref = new SynapseTableReflection($this->db, $this->schema, $table);
        return $ref->getRowsCount();
    }

    public function createTable($tableName, $columns)
    {
        $cols = [];
        /**
         * @var string $column
         * @var string $dataType
         */
        foreach ($columns as $column => $dataType) {
            $cols[] = new SynapseColumn($column, new Synapse($dataType));
        }

        $qb = new SynapseTableQueryBuilder($this->db);
        $this->db->exec($qb->getCreateTableCommand(
            $this->schema,
            $tableName,
            new ColumnCollection($cols)
        ));
    }

    public function fetchAll($table, $style = \PDO::FETCH_NUM, $orderBy = null)
    {
        $data = [];
        $res = $this->db->fetchAll(sprintf(
            "SELECT * FROM %s.%s %s;",
            $this->platform->quoteIdentifier($this->schema),
            $this->platform->quoteIdentifier($table),
            $orderBy !== null ? "ORDER BY $orderBy" : null
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
        $ref = new SynapseTableReflection($this->db, $this->schema, $tableName);
        return $ref->getColumnsDefinitions();
    }
}
