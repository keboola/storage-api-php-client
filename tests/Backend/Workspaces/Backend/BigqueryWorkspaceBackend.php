<?php

namespace Keboola\Test\Backend\Workspaces\Backend;

use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageApi\Exception;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Schema\Bigquery\BigquerySchemaReflection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use PDO;

class BigqueryWorkspaceBackend implements WorkspaceBackend
{
    private BigQueryClient $bqClient;

    private string $schema;

    /**
     * @param array $workspace
     */
    public function __construct(array $workspace)
    {
        $bqClient = new BigQueryClient([
            'keyFile' => $workspace['connection']['credentials'],
        ]);

        assert($bqClient instanceof BigQueryClient);
        $this->bqClient = $bqClient;
        $this->schema = $workspace['connection']['schema'];
    }

    /**
     * @return BigQueryClient
     */
    public function getDb()
    {
        return $this->bqClient;
    }

    public function executeQuery(string $sql): void
    {
        $this->bqClient->runQuery($this->bqClient->query($sql));
    }

    /**
     * @return string[]
     */
    public function getTables()
    {
        $ref = new BigquerySchemaReflection($this->getDb(), $this->schema);
        return $ref->getTablesNames();
    }

    /**
     * @param string $tableName
     * @param array $columns
     * @return void
     * @throws \Keboola\Datatype\Definition\Exception\InvalidLengthException
     * @throws \Keboola\Datatype\Definition\Exception\InvalidOptionException
     * @throws \Keboola\Datatype\Definition\Exception\InvalidTypeException
     */
    public function createTable($tableName, $columns)
    {
        $cols = [];
        /**
         * @var string $column
         * @var string $dataType
         */
        foreach ($columns as $column => $dataType) {
            $cols[] = new BigqueryColumn($column, new Bigquery($dataType));
        }

        $qb = new BigqueryTableQueryBuilder();
        $this->bqClient->runQuery($this->bqClient->query($qb->getCreateTableCommand(
            $this->schema,
            $tableName,
            new ColumnCollection($cols)
        )));
    }

    /**
     * @param string $table
     * @return string[]
     */
    public function getTableColumns($table)
    {
        $ref = new BigqueryTableReflection($this->getDb(), $this->schema, $table);
        return $ref->getColumnsNames();
    }

    /**
     * @param string $table
     * @return void
     */
    public function dropTable($table)
    {
        throw new Exception('TODO Not implemented yet');
    }

    /**
     * @param string $table
     * @return void
     */
    public function dropTableIfExists($table)
    {
        if ($this->isTableExists($this->schema, $table)) {
            $dataset = $this->bqClient->dataset($this->schema);
            $dataset->table($table)->delete();
        }
    }

    /**
     * @param string $table
     * @param string $column
     * @return void
     */
    public function dropTableColumn($table, $column)
    {
        throw new Exception('TODO Not implemented yet');
    }

    /**
     * @param string $table
     * @return string[]
     */
    public function countRows($table)
    {
        $ref = new BigquerySchemaReflection($this->getDb(), $this->schema);
        return $ref->getTablesNames();
    }

    /**
     * @param string $table
     * @param int $style
     * @param string $orderBy
     * @return void
     */
    public function fetchAll($table, $style = PDO::FETCH_NUM, $orderBy = null)
    {
        throw new Exception('TODO Not implemented yet');
    }

    /**
     * @param string $item
     * @return void
     */
    public function toIdentifier($item)
    {
        throw new Exception('TODO Not implemented yet');
    }

    /**
     * @param string $tableName
     * @return void
     */
    public function describeTableColumns($tableName)
    {
        throw new Exception('TODO Not implemented yet');
    }

    protected function isTableExists(string $schema, string $table): bool
    {
        $dataset = $this->bqClient->dataset($schema);
        $table = $dataset->table($table);

        return $table->exists();
    }

    public function dropViewIfExists(string $table): void
    {
        // TODO: Implement dropViewIfExists() method.
    }
}
