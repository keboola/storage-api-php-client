<?php
/**
 * Created by PhpStorm.
 * User: marc
 * Date: 27/07/2016
 * Time: 13:22
 */

namespace Keboola\Test\Backend\Workspaces\Backend;

use Doctrine\DBAL\Connection as DBALConnection;
use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\Db\Import\Snowflake\Connection as SnowflakeConnection;
use Keboola\TableBackendUtils\Schema\SchemaReflectionInterface;
use Keboola\TableBackendUtils\Schema\Teradata\TeradataSchemaReflection;
use Keboola\TableBackendUtils\Table\TableReflectionInterface;
use Keboola\TableBackendUtils\View\ViewReflectionInterface;
use PDO;

interface WorkspaceBackend
{
    /**
     * @return DBALConnection|SnowflakeConnection|PDO|BigQueryClient
     */
    public function getDb();

    public function executeQuery(string $sql): void;

    public function getTables();

    public function createTable($tableName, $columns);

    public function getTableColumns($table);

    public function dropTable($table);

    public function dropTableIfExists($table);

    public function dropViewIfExists(string $table): void;

    public function dropTableColumn($table, $column);

    public function countRows($table);

    /**
     * @param string $table
     * @param int $style
     * @param string|null $orderBy
     */
    public function fetchAll($table, $style = PDO::FETCH_NUM, $orderBy = null);

    // This will return the identifier as it would be returned by the backend.
    public function toIdentifier($item);

    public function describeTableColumns($tableName);

    public function getTableReflection(string $tableName): TableReflectionInterface;

    public function getViewReflection(string $viewName): ViewReflectionInterface;

    public function getSchemaReflection(): SchemaReflectionInterface;
}
