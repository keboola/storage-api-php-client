<?php

namespace Keboola\Test\Backend\Workspaces\Backend;

use Doctrine\DBAL\Connection;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Schema\Snowflake\SnowflakeSchemaReflection;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
use Keboola\TableBackendUtils\View\Snowflake\SnowflakeViewReflection;
use Keboola\Test\Backend\WorkspaceConnectionTrait;

class SnowflakeWorkspaceBackendDBAL implements WorkspaceBackend
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

    /**
     * @param array<mixed> $workspace
     */
    public function __construct(array $workspace)
    {
        $db = $this->getDbConnectionSnowflakeDBAL($workspace['connection']);
        assert($db instanceof Connection);
        $this->db = $db;
        $this->schema = $workspace['connection']['schema'];
    }

    public function executeQuery(string $sql): void
    {
        $this->db->executeQuery($sql);
    }

    /**
     * @param string $table
     * @return string[] of column names
     */
    public function getTableColumns($table): array
    {
        $ref = new SnowflakeTableReflection($this->db, $this->schema, $table);
        return $ref->getColumnsNames();
    }

    /**
     * @return string[] of table names
     */
    public function getTables()
    {
        $ref = new SnowflakeSchemaReflection($this->db, $this->schema);
        return $ref->getTablesNames();
    }

    /**
     * @param string $table
     */
    public function dropTable($table): void
    {
        $qb = new SnowflakeTableQueryBuilder();
        $this->db->executeStatement($qb->getDropTableCommand($this->schema, $table));
    }

    /**
     * @param string $table
     * @param string $column
     */
    public function dropTableColumn($table, $column): void
    {
        $this->db->executeStatement(sprintf(
            'ALTER TABLE %s DROP COLUMN %s;',
            $this->db->quoteIdentifier($table),
            $this->db->quoteIdentifier($column)
        ));
    }

    /**
     * @param string $table
     */
    public function countRows($table): int
    {
        $ref = new SnowflakeTableReflection($this->db, $this->schema, $table);
        return $ref->getRowsCount();
    }

    /**
     * @param string $tableName
     * @param string[] $columns
     */
    public function createTable($tableName, $columns): void
    {
        $cols = [];
        /**
         * @var string $column
         * @var string $dataType
         */
        foreach ($columns as $column => $dataType) {
            $cols[] = new SnowflakeColumn($column, new Snowflake($dataType));
        }

        $qb = new SnowflakeTableQueryBuilder();
        $this->db->executeStatement($qb->getCreateTableCommand(
            $this->schema,
            $tableName,
            new ColumnCollection($cols)
        ));
    }

    /**
     * @param string $table
     * @param int $style
     * @param ?string $orderBy
     * @return array|array[]
     */
    public function fetchAll($table, $style = \PDO::FETCH_NUM, $orderBy = null): array
    {
        $data = [];
        /** @var array[] $res */
        $res = $this->db->fetchAllAssociative(sprintf(
            'SELECT * FROM %s.%s %s;',
            SnowflakeQuote::quoteSingleIdentifier($this->schema),
            SnowflakeQuote::quoteSingleIdentifier($table),
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
        }
        return $data;
    }

    /**
     * @param string $item
     */
    public function toIdentifier($item): string
    {
        return $item;
    }

    /**
     * @param string $tableName
     */
    public function describeTableColumns($tableName): ColumnCollection
    {
        $ref = new SnowflakeTableReflection($this->db, $this->schema, $tableName);
        return $ref->getColumnsDefinitions();
    }

    public function getTableReflection(string $tableName): SnowflakeTableReflection
    {
        return new SnowflakeTableReflection($this->db, $this->schema, $tableName);
    }

    public function getViewReflection(string $tableName): SnowflakeViewReflection
    {
        return new SnowflakeViewReflection($this->db, $this->schema, $tableName);
    }

    public function getSchemaReflection(): SnowflakeSchemaReflection
    {
        return new SnowflakeSchemaReflection($this->db, $this->schema);
    }

    public function disconnect(): void
    {
        $this->db->close();
    }

    /**
     * @param string $table
     */
    public function dropTableIfExists($table): void
    {
        $this->db->executeStatement(sprintf(
            'DROP TABLE IF EXISTS %s.%s;',
            $this->db->quoteIdentifier($this->schema),
            $this->db->quoteIdentifier($table)
        ));
    }

    public function dropViewIfExists(string $table): void
    {
        $this->db->executeStatement(sprintf(
            'DROP VIEW IF EXISTS %s.%s;',
            $this->db->quoteIdentifier($this->schema),
            $this->db->quoteIdentifier($table)
        ));
    }
}
