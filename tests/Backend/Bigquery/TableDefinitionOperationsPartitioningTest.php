<?php

namespace Keboola\Test\Backend\Bigquery;

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
                // todo: expected one partition
            ],
        ], $tableResponse['definition']);
    }
}
