<?php

namespace Keboola\Test\Backend\Bigquery;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\BigqueryWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;

class TableDefinitionOperationsPartitioningTest extends ParallelWorkspacesTestCase
{
    use TestExportDataProvidersTrait;

    protected string $tableId;

    public function setUp(): void
    {
        parent::setUp();
    }

    private function createTableDefinition(array $extend): string
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $data = [
            'name' => 'my_new_table',
            'primaryKeysNames' => ['id'],
            'columns' => [
                [
                    'name' => 'id',
                    'definition' => [
                        'type' => 'INT64',
                        'nullable' => false,
                    ],
                ],
                [
                    'name' => 'time',
                    'definition' => [
                        'type' => 'TIMESTAMP',
                        'nullable' => false,
                    ],
                ],
            ],
        ];

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        return $this->_client->createTableDefinition(
            $bucketId,
            array_merge($data, $extend),
        );
    }

    public function testCreateTableWithTimePartitioningAndClustering(): void
    {
        $tableId = $this->createTableDefinition([
            'clustering' => [
                'fields' => ['id'],
            ],
            'timePartitioning' => [
                'type' => 'DAY',
                'field' => 'time',
                'expirationMs' => 1000,
            ],
        ]);

        $tableResponse = $this->_client->getTable($tableId);
        $expectedTableDefinition = [
            'primaryKeysNames' => [
                'id',
            ],
            'columns' => [
                [
                    'name' => 'id',
                    'definition' => [
                        'type' => 'INTEGER',
                        'nullable' => false,
                    ],
                    'basetype' => 'INTEGER',
                    'canBeFiltered' => true,
                ],
                [
                    'name' => 'time',
                    'definition' => [
                        'type' => 'TIMESTAMP',
                        'nullable' => false,
                    ],
                    'basetype' => 'TIMESTAMP',
                    'canBeFiltered' => true,
                ],
            ],
            'timePartitioning' => [
                'type' => 'DAY',
                'field' => 'time',
                'expirationMs' => '1000',
            ],
            'clustering' => [
                'fields' => [
                    'id',
                ],
            ],
            'requirePartitionFilter' => false,
            'partitions' => [],
        ];
        $this->assertSame($expectedTableDefinition, $tableResponse['definition']);

        $csvFile = $this->createTempCsv();
        $csvFile->writeRow([
            'id',
            'time',
        ]);
        $csvFile->writeRow([
            '1',
            '2020-01-01 00:00:00',
        ]);
        $this->_client->writeTableAsync($tableId, $csvFile);

        $tableResponse = $this->_client->getTable($tableId);
        $this->assertSame(
            array_merge(
                $expectedTableDefinition,
                [
                    'partitions' => [
                        // todo: expected one partition https://keboola.atlassian.net/browse/BIG-186
                        // same apply for partitions after snapshot restore
                    ],
                ],
            ),
            $tableResponse['definition'],
        );

        // test snapshots
        $snapshotId = $this->_client->createTableSnapshot($tableId);
        $newTableId = $this->_client->createTableFromSnapshot($tableResponse['bucket']['id'], $snapshotId, 'restored');
        $newTable = $this->_client->getTable($newTableId);
        $this->assertSame($expectedTableDefinition, $newTable['definition']);

        $workspace = $this->initTestWorkspace();
        // test workspace load
        // if we would implement copy load this needs to be also tested
        $workspaces = new Workspaces($this->_client);
        try {
            $workspaces->loadWorkspaceData($workspace['id'], [
                'input' => [
                    [
                        'source' => $tableId,
                        'destination' => 'partitioned',
                    ],
                ],
            ]);
            $this->fail('Someone probably implemented copy in BQ, well fix this test mate.');
        } catch (ClientException $e) {
            $this->assertSame('Backend "bigquery" does not support: "Other types of loading than view".', $e->getMessage());
        }

        // test unload table from WS
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        assert($backend instanceof BigqueryWorkspaceBackend);

        $client = $backend->getDb();

        $dataset = $client->dataset($workspace['connection']['schema']);
        $tableInWorkspace = $dataset->table('partitioned');
        if ($tableInWorkspace->exists()) {
            $tableInWorkspace->delete();
        }
        $dataset->createTable('partitioned', [
            'schema' => [
                'fields' => [
                    [
                        'name' => 'id',
                        'type' => 'INTEGER',
                        'mode' => 'REQUIRED',
                    ],
                    [
                        'name' => 'time',
                        'type' => 'TIMESTAMP',
                        'mode' => 'REQUIRED',
                    ],
                ],
            ],
            'timePartitioning' => [
                'type' => 'DAY',
                'field' => 'time',
                'expirationMs' => 1000,
            ],
            'clustering' => [
                'fields' => ['id'],
            ],
        ]);

        // unload table back to storage
        $unloadedTableId = $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), [
            'name' => 'partitionedUnload',
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'partitioned',
        ]);
        $unloadedTable = $this->_client->getTable($unloadedTableId);
        // table created by unload from WS are created without type
        $this->assertArrayNotHasKey('definition', $unloadedTable);
    }

    public function testErrorWhenCreatingTableWithPartitioning(): void
    {
        try {
            // creating table with clustering with wrong field
            $this->createTableDefinition([
                'clustering' => [
                    'fields' => ['not exists'],
                ],
            ]);
            $this->fail('Exception should be thrown');
        } catch (ClientException $e) {
            $this->assertInstanceOf(ClientException::class, $e);
            $this->assertSame('storage.tables.validation', $e->getStringCode());
            $this->assertMatchesRegularExpression(
                '/Failed to create table "my_new_table" in dataset ".*"\. Exception: The field specified for clustering cannot be found in the schema\..*/',
                $e->getMessage(),
            );
        }
    }
}
