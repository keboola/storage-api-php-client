<?php

namespace Keboola\Test\Backend\Workspaces\Backend;

use Doctrine\DBAL\Connection;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageApi\Exception;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Schema\Teradata\TeradataSchemaReflection;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use Keboola\Test\Backend\WorkspaceConnectionTrait;

/**
 * @method Connection getDbConnection(array $connection)
 */
class TeradataWorkspaceBackend implements WorkspaceBackend
{
    use WorkspaceConnectionTrait;

    /** @var Connection */
    private $db;

    /** @var string */
    private $schema;

    /**
     * @param array $workspace
     */
    public function __construct($workspace)
    {
        $this->db = $this->getDbConnection($workspace['connection']);
        $this->schema = $workspace['connection']['schema'];
    }

    /**
     * @return Connection
     */
    private function getDb()
    {
        return $this->db;
    }

    /**
     * @return void
     */
    public function disconnect()
    {
        $this->db->close();
    }

    /**
     * @param string $table
     * @return array of column names
     */
    public function getTableColumns($table)
    {
        $ref = new TeradataTableReflection($this->getDb(), $this->schema, $table);
        return $ref->getColumnsNames();
    }

    /**
     * @return array of table names
     */
    public function getTables()
    {
        $ref = new TeradataSchemaReflection($this->getDb(), $this->schema);
        return $ref->getTablesNames();
    }

    /**
     * @param string $table
     * @return void
     */
    public function dropTable($table)
    {
        $qb = new TeradataTableQueryBuilder();
        $this->getDb()->executeStatement($qb->getDropTableCommand($this->schema, $table));
    }

    /**
     * @param string $table
     * @param string $column
     * @return void
     */
    public function dropTableColumn($table, $column)
    {
        $this->getDb()->executeStatement(sprintf(
            "ALTER TABLE %s.%s DROP %s;",
            TeradataQuote::quoteSingleIdentifier($this->schema),
            TeradataQuote::quoteSingleIdentifier($table),
            TeradataQuote::quoteSingleIdentifier($column)
        ));
    }

    /**
     * @param string $table
     * @return int
     */
    public function countRows($table)
    {
        $ref = new TeradataTableReflection($this->getDb(), $this->schema, $table);
        return $ref->getRowsCount();
    }

    /**
     * @param string $tableName
     * @param array $columns
     * @return void
     */
    public function createTable($tableName, $columns)
    {
        $cols = [];
        /**
         * @var string $column
         * @var string $dataType
         */
        foreach ($columns as $column => $dataType) {
            $cols[] = new TeradataColumn($column, new Teradata($dataType));
        }

        $qb = new TeradataTableQueryBuilder();
        $this->getDb()->executeStatement($qb->getCreateTableCommand(
            $this->schema,
            $tableName,
            new ColumnCollection($cols)
        ));
    }

    /**
     * @param string $table
     * @param int $style
     * @param string $orderBy
     * @return array
     */
    public function fetchAll($table, $style = \PDO::FETCH_NUM, $orderBy = null)
    {
        $data = [];
        $res = $this->getDb()->fetchAllAssociative(sprintf(
            "SELECT * FROM %s.%s %s;",
            TeradataQuote::quoteSingleIdentifier($this->schema),
            TeradataQuote::quoteSingleIdentifier($table),
            $orderBy !== null ? "ORDER BY $orderBy" : null
        ));
        switch ($style) {
            case \PDO::FETCH_NUM:
                /** @var array $row */
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
     * @param mixed $item
     * @return mixed
     */
    public function toIdentifier($item)
    {
        return $item;
    }

    /**
     * @param string $tableName
     * @return ColumnCollection
     */
    public function describeTableColumns($tableName)
    {
        $ref = new TeradataTableReflection($this->getDb(), $this->schema, $tableName);
        return $ref->getColumnsDefinitions();
    }

    /**
     * @param string $tableName
     * @return TeradataTableReflection
     */
    public function getTableReflection($tableName)
    {
        return new TeradataTableReflection($this->getDb(), $this->schema, $tableName);
    }

    /**
     * @param string $tableName
     * @return void
     */
    public function getViewReflection($tableName)
    {
        throw new Exception('TODO Not implemented yet');
    }

    /**
     * @return TeradataSchemaReflection
     */
    public function getSchemaReflection()
    {
        return new TeradataSchemaReflection($this->getDb(), $this->schema);
    }

    /**
     * @param string $table
     * @return void
     */
    public function dropTableIfExists($table)
    {
        if ($this->isTableExists($this->schema, $table)) {
            $this->getDb()->executeStatement(sprintf(
                "DROP TABLE %s.%s;",
                TeradataQuote::quoteSingleIdentifier($this->schema),
                TeradataQuote::quoteSingleIdentifier($table)
            ));
        }
    }

    /**
     * @param string $databaseName
     * @param string $tableName
     * @return bool
     */
    protected function isTableExists($databaseName, $tableName)
    {
        $tables = $this->getDb()->fetchAllAssociative(sprintf(
            'SELECT TableName FROM DBC.TablesVX WHERE DatabaseName = %s AND TableName = %s',
            TeradataQuote::quote($databaseName),
            TeradataQuote::quote($tableName)
        ));
        return count($tables) === 1;
    }

    public function __destruct()
    {
        $this->disconnect();
    }
}
