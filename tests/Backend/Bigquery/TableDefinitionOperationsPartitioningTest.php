<?php

namespace Keboola\Test\Backend\Bigquery;

use Keboola\StorageApi\ClientException;
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
            array_merge($data, $extend)
        );
    }

    public function testCreateTableWithTimePartitioningAndClustering(): void
    {
        $tableId = $this->createTableDefinition([
            'clustering' => [
                'fields' => ['id'],
            ],
            'requirePartitionFilter' => true,
            'timePartitioning' => [
                'type' => 'DAY',
                'field' => 'time',
                'expirationMs' => 1000,
            ],
        ]);

        $tableResponse = $this->_client->getTable($tableId);
        $this->assertSame([
            'primaryKeysNames' => [
                0 => 'id',
            ],
            'columns' => [
                0 => [
                    'name' => 'id',
                    'definition' => [
                        'type' => 'INTEGER',
                        'nullable' => false,
                    ],
                    'basetype' => 'INTEGER',
                    'canBeFiltered' => true,
                ],
                1 => [
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
                    0 => 'id',
                ],
            ],
            'requirePartitionFilter' => true,
            'partitions' => [],
        ], $tableResponse['definition']);

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
        $this->assertSame([
            'primaryKeysNames' => [
                0 => 'id',
            ],
            'columns' => [
                0 => [
                    'name' => 'id',
                    'definition' => [
                        'type' => 'INTEGER',
                        'nullable' => false,
                    ],
                    'basetype' => 'INTEGER',
                    'canBeFiltered' => true,
                ],
                1 => [
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
                    0 => 'id',
                ],
            ],
            'requirePartitionFilter' => true,
            'partitions' => [
                // todo: expected one partition https://keboola.atlassian.net/browse/BIG-186
            ],
        ], $tableResponse['definition']);
    }

    public function testErrorWhenCreatingTableWithPartitioning(): void
    {
        try {
            // creating table with clustering and no partitioning
            $this->createTableDefinition([
                'clustering' => [
                    'fields' => ['id'],
                ],
                'requirePartitionFilter' => true,
            ]);
            $this->fail('Exception should be thrown');
        } catch (ClientException $e) {
            $this->assertInstanceOf(ClientException::class, $e);
            $this->assertSame('storage.tables.validation', $e->getStringCode());
            $this->assertMatchesRegularExpression(
                '/Failed to create table "my_new_table" in dataset ".*"\. Exception: Either interval partition or range partition should be specified\..*/',
                $e->getMessage()
            );
        }
    }
}
