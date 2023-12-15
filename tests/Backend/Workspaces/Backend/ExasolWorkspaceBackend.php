<?php

namespace Keboola\Test\Backend\Workspaces\Backend;

use Doctrine\DBAL\Connection;
use Keboola\Datatype\Definition\Exasol;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Exasol\ExasolColumn;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Schema\Exasol\ExasolSchemaReflection;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableReflection;
use Keboola\TableBackendUtils\View\Exasol\ExasolViewReflection;
use Keboola\Test\Backend\WorkspaceConnectionTrait;

class ExasolWorkspaceBackend implements WorkspaceBackend
{
    use WorkspaceConnectionTrait;

    private Connection $db;

    private string $schema;

    public function __construct($workspace)
    {
        $this->db = $this->getDbConnection($workspace['connection']);
        $this->schema = $workspace['connection']['schema'];
    }

    /**
     * @return Connection
     */
    public function getDb()
    {
        return $this->db;
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
        $ref = new ExasolTableReflection($this->db, $this->schema, $table);
        return $ref->getColumnsNames();
    }

    /**
     * @return array of table names
     */
    public function getTables()
    {
        $ref = new ExasolSchemaReflection($this->db, $this->schema);
        return $ref->getTablesNames();
    }

    public function dropTable($table)
    {
        $qb = new ExasolTableQueryBuilder();
        $this->db->executeStatement($qb->getDropTableCommand($this->schema, $table));
    }

    public function dropTableColumn($table, $column)
    {
        $this->db->executeStatement(sprintf(
            'ALTER TABLE %s.%s DROP COLUMN %s;',
            ExasolQuote::quoteSingleIdentifier($this->schema),
            ExasolQuote::quoteSingleIdentifier($table),
            ExasolQuote::quoteSingleIdentifier($column),
        ));
    }

    public function countRows($table)
    {
        $ref = new ExasolTableReflection($this->db, $this->schema, $table);
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
            $cols[] = new ExasolColumn($column, new Exasol($dataType));
        }

        $qb = new ExasolTableQueryBuilder();
        $this->db->executeStatement($qb->getCreateTableCommand(
            $this->schema,
            $tableName,
            new ColumnCollection($cols),
        ));
    }

    public function fetchAll($table, $style = \PDO::FETCH_NUM, $orderBy = null)
    {
        $data = [];
        $res = $this->db->fetchAllAssociative(sprintf(
            'SELECT * FROM %s.%s %s;',
            ExasolQuote::quoteSingleIdentifier($this->schema),
            ExasolQuote::quoteSingleIdentifier($table),
            $orderBy !== null ? "ORDER BY $orderBy" : null,
        ));
        switch ($style) {
            case \PDO::FETCH_NUM:
                foreach ($res as $row) {
                    $data[] = array_values((array) $row);
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
        $ref = new ExasolTableReflection($this->db, $this->schema, $tableName);
        return $ref->getColumnsDefinitions();
    }

    public function getTableReflection(string $tableName): ExasolTableReflection
    {
        return new ExasolTableReflection($this->db, $this->schema, $tableName);
    }

    public function getViewReflection(string $viewName): ExasolViewReflection
    {
        return new ExasolViewReflection($this->db, $this->schema, $viewName);
    }

    public function getSchemaReflection(): ExasolSchemaReflection
    {
        return new ExasolSchemaReflection($this->db, $this->schema);
    }

    public function disconnect()
    {
        $this->db->close();
    }

    public function dropTableIfExists($table)
    {
        $this->db->executeStatement(sprintf(
            'DROP TABLE IF EXISTS %s.%s;',
            ExasolQuote::quoteSingleIdentifier($this->schema),
            ExasolQuote::quoteSingleIdentifier($table),
        ));
    }

    public function dropViewIfExists(string $table): void
    {
        // TODO: Implement dropViewIfExists() method.
    }
}
