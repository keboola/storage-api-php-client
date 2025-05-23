<?php

namespace Keboola\Test\Backend\Workspaces\Backend;

use Doctrine\DBAL\Connection;
use Keboola\Datatype\Definition\Synapse;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\Escaping\SynapseQuote;
use Keboola\TableBackendUtils\Schema\SynapseSchemaReflection;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;
use Keboola\TableBackendUtils\View\SynapseViewReflection;
use Keboola\Test\Backend\WorkspaceConnectionTrait;

class SynapseWorkspaceBackend implements WorkspaceBackend
{
    use WorkspaceConnectionTrait;

    private Connection $db;

    private string $schema;

    public function __construct($workspace)
    {
        $db = $this->getDbConnection($workspace['connection']);
        assert($db instanceof Connection);
        $this->db = $db;
        $this->schema = $workspace['connection']['schema'];
    }

    public function executeQuery(string $sql): void
    {
        $this->db->executeQuery($sql);
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
     * @return string[] of table names
     */
    public function getTables(): array
    {
        $ref = new SynapseSchemaReflection($this->db, $this->schema);
        return $ref->getTablesNames();
    }

    public function dropTable($table)
    {
        $qb = new SynapseTableQueryBuilder();
        $this->db->executeStatement($qb->getDropTableCommand($this->schema, $table));
    }

    public function dropTableColumn($table, $column)
    {
        $this->db->executeStatement(sprintf(
            'ALTER TABLE %s.%s DROP COLUMN %s;',
            SynapseQuote::quoteSingleIdentifier($this->schema),
            SynapseQuote::quoteSingleIdentifier($table),
            SynapseQuote::quoteSingleIdentifier($column),
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

        $qb = new SynapseTableQueryBuilder();
        $this->db->executeStatement($qb->getCreateTableCommand(
            $this->schema,
            $tableName,
            new ColumnCollection($cols),
        ));
    }

    public function fetchAll($table, $style = \PDO::FETCH_NUM, $orderBy = null)
    {
        $data = [];
        /** @var array[] $res */
        $res = $this->db->fetchAllAssociative(sprintf(
            'SELECT * FROM %s.%s %s;',
            SynapseQuote::quoteSingleIdentifier($this->schema),
            SynapseQuote::quoteSingleIdentifier($table),
            $orderBy !== null ? "ORDER BY $orderBy" : null,
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

    public function getTableReflection(string $tableName): SynapseTableReflection
    {
        return new SynapseTableReflection($this->db, $this->schema, $tableName);
    }

    public function getViewReflection(string $viewName): SynapseViewReflection
    {
        return new SynapseViewReflection($this->db, $this->schema, $viewName);
    }

    public function getSchemaReflection(): SynapseSchemaReflection
    {
        return new SynapseSchemaReflection($this->db, $this->schema);
    }

    public function disconnect()
    {
        $this->db->close();
    }

    public function dropTableIfExists($table)
    {
        $this->db->executeStatement(sprintf(
            "IF OBJECT_ID (N'%s.%s', N'U') IS NOT NULL DROP TABLE %s.%s;",
            SynapseQuote::quoteSingleIdentifier($this->schema),
            SynapseQuote::quoteSingleIdentifier($table),
            SynapseQuote::quoteSingleIdentifier($this->schema),
            SynapseQuote::quoteSingleIdentifier($table),
        ));
    }

    public function getDb()
    {
        return $this->db;
    }

    public function dropViewIfExists(string $table): void
    {
        // TODO: Implement dropViewIfExists() method.
    }
}
