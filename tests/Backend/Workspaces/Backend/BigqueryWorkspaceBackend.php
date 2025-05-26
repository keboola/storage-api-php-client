<?php

namespace Keboola\Test\Backend\Workspaces\Backend;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Dataset;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageApi\Exception;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Connection\Bigquery\BigQueryClientWrapper;
use Keboola\TableBackendUtils\Connection\Bigquery\Retry;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Schema\Bigquery\BigquerySchemaReflection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Keboola\TableBackendUtils\View\ViewReflectionInterface;
use PDO;
use Psr\Log\NullLogger;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;

class BigqueryWorkspaceBackend implements WorkspaceBackend
{
    private BigQueryClientWrapper $bqClient;

    private string $schema;

    /**
     * @param array $workspace
     */
    public function __construct(array $workspace)
    {
        $bqClient = new BigQueryClientWrapper([
            'keyFile' => $workspace['connection']['credentials'],
            'restRetryFunction' => Retry::getRestRetryFunction(new NullLogger(), true),
            'requestTimeout' => 120,
            'retries' => 20,
        ], 'sapitest');

        $this->bqClient = $bqClient;
        $this->schema = $workspace['connection']['schema'];
    }

    public function getDataset(): Dataset
    {
        return $this->bqClient->dataset($this->schema);
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
        $this->bqClient->runQuery($this->bqClient->query($sql, [
            'configuration' => [
                'defaultDataset' => [
                    'datasetId' => $this->schema,
                ],
            ],
        ]));
    }

    /**
     * @return string[] of table names
     */
    public function getTables(): array
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
            new ColumnCollection($cols),
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
        $this->executeQuery(sprintf(
            'DROP TABLE %s.%s',
            BigqueryQuote::quoteSingleIdentifier($this->schema),
            BigqueryQuote::quoteSingleIdentifier($table),
        ));
    }

    public function dropView(string $viewName): void
    {
        $this->executeQuery(sprintf('DROP VIEW %s.%s', $this->schema, $viewName));
    }

    /**
     * @param string $table
     * @return void
     */
    public function dropTableIfExists($table)
    {
        if ($this->isTableExists($this->schema, $table)) {
            $dataset = $this->bqClient->dataset($this->schema);
            $dataset->table($table)->delete(['retries' => 5]);
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
     * @return array<mixed>
     */
    public function fetchAll($table, $style = PDO::FETCH_NUM, $orderBy = null)
    {
        $query = $this->bqClient->query(
            sprintf(
                'SELECT * FROM %s.%s%s;',
                BigqueryQuote::quoteSingleIdentifier($this->schema),
                BigqueryQuote::quoteSingleIdentifier($table),
                $orderBy !== null ? " ORDER BY $orderBy" : null,
            ),
        );
        $queryResults = $this->bqClient->runQuery($query);

        $data = [];
        switch ($style) {
            case \PDO::FETCH_NUM:
                foreach ($queryResults as $row) {
                    // @phpstan-ignore-next-line
                    $data[] = array_values($row);
                }
                break;
            case \PDO::FETCH_ASSOC:
                $data = $queryResults;
                break;
            default:
                throw new \Exception("Unknown fetch style $style");
        }
        return (array) $data;
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

        $exists = (new RetryProxy(new SimpleRetryPolicy(10)))->call(function () use ($table) {
            return $table->exists();
        });
        assert(is_bool($exists));
        return $exists;
    }

    public function dropViewIfExists(string $table): void
    {
        // TODO: Implement dropViewIfExists() method.
    }

    public function getTableReflection(string $tableName): BigqueryTableReflection
    {
        return new BigQueryTableReflection($this->bqClient, $this->schema, $tableName);
    }

    public function getViewReflection(string $tableName): ViewReflectionInterface
    {
        throw new \Exception('TODO Not implemented yet');
    }

    public function getSchemaReflection(): BigquerySchemaReflection
    {
        return new BigquerySchemaReflection($this->bqClient, $this->schema);
    }
}
