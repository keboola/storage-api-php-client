<?php

namespace Keboola\Test\Backend\Workspaces\Backend;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\TableBackendUtils\Schema\SchemaReflectionInterface;
use Keboola\TableBackendUtils\Table\TableReflectionInterface;
use Keboola\TableBackendUtils\View\ViewReflectionInterface;
use Keboola\Test\Backend\WorkspaceConnectionTrait;

class SnowflakeWorkspaceBackend implements WorkspaceBackend
{
    use WorkspaceConnectionTrait;

    private Connection $db;

    private string $schema;

    /**
     * @return Connection
     */
    public function getDb()
    {
        return $this->db;
    }

    public function __construct($workspace)
    {
        $db = $this->getDbConnection($workspace['connection']);
        assert($db instanceof Connection);
        $this->db = $db;
        $this->schema = $workspace['connection']['schema'];
    }

    public function executeQuery(string $sql): void
    {
        $this->db->query($sql);
    }

    /**
     * @param $table
     * @return array of column names
     */
    public function getTableColumns($table)
    {
        return array_map(function ($column) {
            return $column['column_name'];
        }, $this->db->fetchAll(sprintf('SHOW COLUMNS IN %s', $this->db->quoteIdentifier($table))));
    }

    /**
     * @return string[] of table names
     */
    public function getTables(): array
    {
        /** @var string[] $names */
        $names = array_map(
            function (array $table) {
                return $table['name'];
            },
            $this->db->fetchAll(sprintf('SHOW TABLES IN SCHEMA %s', $this->db->quoteIdentifier($this->schema))),
        );

        return $names;
    }

    public function dropTable($table)
    {
        $this->db->query(sprintf('DROP TABLE %s;', $this->db->quoteIdentifier($table)));
    }

    public function dropTableColumn($table, $column)
    {
        $this->db->query(sprintf(
            'ALTER TABLE %s DROP COLUMN %s;',
            $this->db->quoteIdentifier($table),
            $this->db->quoteIdentifier($column),
        ));
    }

    public function countRows($table)
    {
        $tables = $this->db->fetchAll(
            sprintf("SHOW TABLES LIKE '%s' IN SCHEMA %s", $table, $this->schema),
        );
        return (int) $tables[0]['rows'];
    }

    public function createTable($tableName, $columns)
    {
        $cols = [];
        foreach ($columns as $column => $dataType) {
            $cols[] = $this->db->quoteIdentifier($column) . ' ' . $dataType;
        }
        $qry = sprintf('CREATE TABLE %s (%s)', $this->db->quoteIdentifier($tableName), implode(', ', $cols));
        $this->db->query($qry);
    }

    public function fetchAll($table, $style = \PDO::FETCH_NUM, $orderBy = null)
    {
        $data = [];
        $res = $this->db->fetchAll(sprintf(
            'SELECT * FROM %s.%s %s;',
            $this->db->quoteIdentifier($this->schema),
            $this->db->quoteIdentifier($table),
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
        return $this->db->fetchAll(sprintf('DESC TABLE %s.%s', $this->db->quoteIdentifier($this->schema), $this->db->quoteIdentifier($tableName)));
    }

    public function dropTableIfExists($table)
    {
        $this->db->query(sprintf(
            'DROP TABLE IF EXISTS %s.%s;',
            $this->db->quoteIdentifier($this->schema),
            $this->db->quoteIdentifier($table),
        ));
    }

    public function dropViewIfExists(string $table): void
    {
        $this->db->query(sprintf(
            'DROP VIEW IF EXISTS %s.%s;',
            $this->db->quoteIdentifier($this->schema),
            $this->db->quoteIdentifier($table),
        ));
    }

    public function getTableReflection(string $tableName): TableReflectionInterface
    {
        throw new \Exception('TODO Not implemented yet');
    }

    public function getViewReflection(string $viewName): ViewReflectionInterface
    {
        throw new \Exception('TODO Not implemented yet');
    }

    public function getSchemaReflection(): SchemaReflectionInterface
    {
        throw new \Exception('TODO Not implemented yet');
    }
}
