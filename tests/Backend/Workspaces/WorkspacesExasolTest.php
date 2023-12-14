<?php

namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Csv\CsvFile;
use Keboola\TableBackendUtils\Column\Exasol\ExasolColumn;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class WorkspacesExasolTest extends ParallelWorkspacesTestCase
{
    use WorkspaceConnectionTrait;

    public function testCreateNotSupportedBackend(): void
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        try {
            $workspaces->createWorkspace(['backend' => self::BACKEND_REDSHIFT]);
            $this->fail('should not be able to create WS for unsupported backend');
        } catch (ClientException $e) {
            $this->assertEquals($e->getStringCode(), 'workspace.backendNotSupported');
        }
    }

    public function testLoadDataTypesDefaults(): void
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

        // Create a columns of sample data
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $table1Id = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($importFile),
        );

        $table2Id = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'rates',
            new CsvFile(__DIR__ . '/../../_data/rates.csv'),
        );

        $workspaces->loadWorkspaceData($workspace['id'], [
            'input' => [
                [
                    'source' => $table1Id,
                    'destination' => 'languages',
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'int',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                    ],
                ],
                [
                    'source' => $table2Id,
                    'destination' => 'rates',
                    'columns' => [
                        [
                            'source' => 'Date',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'SKK',
                            'type' => 'varchar',
                        ],
                    ],
                ],
            ],
        ]);

        $actualJobId = null;
        foreach ($this->_client->listJobs() as $job) {
            if ($job['operationName'] === 'workspaceLoad') {
                if ((int) $job['operationParams']['workspaceId'] === $workspace['id']) {
                    $actualJobId = $job;
                }
            }
        }

        $this->assertArrayHasKey('metrics', $actualJobId);
        $this->assertEquals(2678, $actualJobId['metrics']['outBytes']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        /** @var ExasolColumn[] $columns */
        $columns = iterator_to_array($backend->describeTableColumns('languages'));

        $this->assertEquals('id', $columns[0]->getColumnName());
        $this->assertEquals('DECIMAL (18,0)', $columns[0]->getColumnDefinition()->getSQLDefinition());

        $this->assertEquals('name', $columns[1]->getColumnName());
        $this->assertEquals('VARCHAR (2000000)', $columns[1]->getColumnDefinition()->getSQLDefinition());

        $columns = iterator_to_array($backend->describeTableColumns('rates'));

        $this->assertEquals('Date', $columns[0]->getColumnName());
        $this->assertEquals('VARCHAR (2000000)', $columns[0]->getColumnDefinition()->getSQLDefinition());

        $this->assertEquals('SKK', $columns[1]->getColumnName());
        $this->assertEquals('VARCHAR (2000000)', $columns[1]->getColumnDefinition()->getSQLDefinition());
    }

    public function testLoadedPrimaryKeys(): void
    {
        $primaries = ['Paid_Search_Engine_Account','Date','Paid_Search_Campaign','Paid_Search_Ad_ID','Site__DFA'];
        $pkTableId = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languages-pk',
            new CsvFile(__DIR__ . '/../../_data/multiple-columns-pk.csv'),
            [
                'primaryKey' => implode(',', $primaries),
            ],
        );

        $mapping = [
            'source' => $pkTableId,
            'destination' => 'languages-pk',
        ];

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $workspaces->loadWorkspaceData($workspace['id'], ['input' => [$mapping]]);

        /** @var ExasolColumn[] $columns */
        $columns = iterator_to_array($backend->describeTableColumns('languages-pk'));
        $this->assertCount(6, $columns);
        $this->assertEquals('Paid_Search_Engine_Account', $columns[0]->getColumnName());
        $this->assertEquals('VARCHAR (2000000) NOT NULL', $columns[0]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Advertiser_ID', $columns[1]->getColumnName());
        $this->assertEquals('VARCHAR (2000000)', $columns[1]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Date', $columns[2]->getColumnName());
        $this->assertEquals('VARCHAR (2000000) NOT NULL', $columns[2]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Paid_Search_Campaign', $columns[3]->getColumnName());
        $this->assertEquals('VARCHAR (2000000) NOT NULL', $columns[3]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Paid_Search_Ad_ID', $columns[4]->getColumnName());
        $this->assertEquals('VARCHAR (2000000) NOT NULL', $columns[4]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Site__DFA', $columns[5]->getColumnName());
        $this->assertEquals('VARCHAR (2000000) NOT NULL', $columns[5]->getColumnDefinition()->getSQLDefinition());

        // Check that PK is NOT set if not all PK columns are present
        $mapping2 = [
            'source' => $pkTableId,
            'destination' => 'languages-pk-skipped',
            'columns' => [
                [
                    'source' => 'Paid_Search_Engine_Account',
                    'type' => 'varchar',
                ],
                [
                    'source' => 'Date',
                    'type' => 'varchar',
                ],
            ],
        ];
        $workspaces->loadWorkspaceData($workspace['id'], ['input' => [$mapping2]]);

        /** @var ExasolColumn[] $columns */
        $columns = iterator_to_array($backend->describeTableColumns('languages-pk-skipped'));
        $this->assertCount(2, $columns);
        $this->assertEquals('Paid_Search_Engine_Account', $columns[0]->getColumnName());
        $this->assertEquals('VARCHAR (2000000)', $columns[0]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Date', $columns[1]->getColumnName());
        $this->assertEquals('VARCHAR (2000000)', $columns[1]->getColumnDefinition()->getSQLDefinition());
    }

    public function testLoadIncremental(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile($importFile),
            ['primaryKey' => 'id'],
        );

        $importFile = __DIR__ . '/../../_data/languages-more-columns.csv';
        $table2Id = $this->_client->createTableAsync(
            $bucketId,
            'languagesDetails',
            new CsvFile($importFile),
            ['primaryKey' => 'Id'],
        );

        // first load
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'whereColumn' => 'name',
                    'whereValues' => ['czech', 'french'],
                ],
                [
                    'source' => $table2Id,
                    'destination' => 'languagesDetails',
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $actualJobId = null;
        foreach ($this->_client->listJobs() as $job) {
            if ($job['operationName'] === 'workspaceLoad') {
                if ((int) $job['operationParams']['workspaceId'] === $workspace['id']) {
                    $actualJobId = $job;
                }
            }
        }

        $this->assertArrayHasKey('metrics', $actualJobId);
        $this->assertEquals(76, $actualJobId['metrics']['outBytes']);

        $this->assertEquals(2, $backend->countRows('languages'));
        $this->assertEquals(5, $backend->countRows('languagesDetails'));

        // second load
        $options = [
            'input' => [
                [
                    'incremental' => true,
                    'source' => $tableId,
                    'destination' => 'languages',
                    'whereColumn' => 'name',
                    'whereValues' => ['english', 'czech'],
                ],
                [
                    'source' => $table2Id,
                    'destination' => 'languagesDetails',
                    'whereColumn' => 'iso',
                    'whereValues' => ['ff'],
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(3, $backend->countRows('languages'));
        $this->assertEquals(3, $backend->countRows('languagesDetails'));
    }

    public function testLoadIncrementalAndPreserve(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile($importFile),
            ['primaryKey' => 'id'],
        );

        $importFile = __DIR__ . '/../../_data/languages-more-columns.csv';
        $table2Id = $this->_client->createTableAsync(
            $bucketId,
            'languagesDetails',
            new CsvFile($importFile),
            ['primaryKey' => 'Id'],
        );

        // first load
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'whereColumn' => 'name',
                    'whereValues' => ['czech', 'french'],
                ],
                [
                    'source' => $table2Id,
                    'destination' => 'languagesDetails',
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(2, $backend->countRows('languages'));
        $this->assertEquals(5, $backend->countRows('languagesDetails'));

        // second load
        $options = [
            'preserve' => true,
            'input' => [
                [
                    'incremental' => true,
                    'source' => $tableId,
                    'destination' => 'languages',
                    'whereColumn' => 'name',
                    'whereValues' => ['english', 'czech'],
                ],
                [
                    'source' => $table2Id,
                    'destination' => 'languagesDetails',
                    'whereColumn' => 'iso',
                    'whereValues' => ['ff'],
                ],
            ],
        ];

        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('Non incremental load to existing table should fail');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.duplicateTable', $e->getStringCode());
        }
    }

    public function testLoadIncrementalNullable(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../_data/languages.with-state.csv';
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile($importFile),
            ['primaryKey' => 'id'],
        );

        // first load
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'whereColumn' => 'id',
                    'whereValues' => [0, 26, 1],
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'SMALLINT',
                            'nullable' => false,
                        ],
                        [
                            'source' => 'name',
                            'type' => 'VARCHAR',
                            'length' => '50',
                            'nullable' => false,
                        ],
                        [
                            'source' => 'State',
                            'type' => 'VARCHAR',
                            'convertEmptyValuesToNull' => true,
                            'nullable' => true,
                        ],
                    ],
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(3, $backend->countRows('languages'));

        // second load
        $options = [
            'input' => [
                [
                    'incremental' => true,
                    'source' => $tableId,
                    'destination' => 'languages',
                    'whereColumn' => 'id',
                    'whereValues' => [11, 26, 24],
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'SMALLINT',
                            'nullable' => false,
                        ],
                        [
                            'source' => 'name',
                            'type' => 'VARCHAR',
                            'length' => '50',
                            'nullable' => false,
                        ],
                        [
                            'source' => 'State',
                            'type' => 'VARCHAR',
                            'convertEmptyValuesToNull' => true,
                            'nullable' => true,
                        ],
                    ],
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(5, $backend->countRows('languages'));

        $rows = $backend->fetchAll('languages', \PDO::FETCH_ASSOC);
        foreach ($rows as $row) {
            $this->assertArrayHasKey('State', $row);
            $this->assertArrayHasKey('id', $row);

            if (in_array($row['id'], ['0', '11', '24'])) {
                $this->assertNull($row['State']);
            }
        }
    }

    public function testLoadIncrementalNotNullable(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../_data/languages.with-state.csv';
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile($importFile),
            ['primaryKey' => 'id'],
        );

        // first load
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'whereColumn' => 'id',
                    'whereValues' => [26, 1],
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'SMALLINT',
                            'nullable' => false,
                        ],
                        [
                            'source' => 'name',
                            'type' => 'VARCHAR',
                            'length' => '50',
                            'nullable' => false,
                        ],
                        [
                            'source' => 'State',
                            'type' => 'VARCHAR',
                            'convertEmptyValuesToNull' => true,
                            'nullable' => false,
                        ],
                    ],
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(2, $backend->countRows('languages'));

        // second load
        $options = [
            'input' => [
                [
                    'incremental' => true,
                    'source' => $tableId,
                    'destination' => 'languages',
                    'whereColumn' => 'id',
                    'whereValues' => [11, 26, 24],
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'SMALLINT',
                            'nullable' => false,
                        ],
                        [
                            'source' => 'name',
                            'type' => 'VARCHAR',
                            'length' => '50',
                            'nullable' => false,
                        ],
                        [
                            'source' => 'State',
                            'type' => 'VARCHAR',
                            'convertEmptyValuesToNull' => true,
                            'nullable' => false,
                        ],
                    ],
                ],
            ],
        ];

        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('Load columns wit NULL should fail');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.tableLoad', $e->getStringCode());
        }
    }

    /**
     * @dataProvider dataTypesDiffDefinitions
     */
    public function testsIncrementalDataTypesDiff($table, $firstLoadColumns, $secondLoadColumns, $shouldFail): void
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

        $importFile = __DIR__ . "/../../_data/$table.csv";

        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            $table,
            new CsvFile($importFile),
        );

        // first load
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => $table,
                    'columns' => $firstLoadColumns,
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);

        // second load - incremental
        $options = [
            'input' => [
                [
                    'incremental' => true,
                    'source' => $tableId,
                    'destination' => $table,
                    'columns' => $secondLoadColumns,
                ],
            ],
        ];

        if ($shouldFail) {
            try {
                $workspaces->loadWorkspaceData($workspace['id'], $options);
                $this->fail('Incremental load with different datatypes should fail');
            } catch (ClientException $e) {
                $this->assertEquals('workspace.columnsTypesNotMatch', $e->getStringCode());
                $this->assertStringContainsString('Different mapping between', $e->getMessage());
            }
        } else {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
        }
    }

    public function testOutBytesMetricsWithLoadWorkspaceWithRows(): void
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace([], true);

        // Create a table of sample data
        $table1Id = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
        );

        $table2Id = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'rates',
            new CsvFile(__DIR__ . '/../../_data/rates.csv'),
        );

        $workspaces->loadWorkspaceData($workspace['id'], [
            'input' => [
                [
                    'source' => $table1Id,
                    'destination' => 'languages',
                ],
                [
                    'source' => $table2Id,
                    'destination' => 'rates',
                    'rows' => 15,
                ],
            ],
        ]);

        $actualJobId = null;
        foreach ($this->_client->listJobs() as $job) {
            if ($job['operationName'] === 'workspaceLoad') {
                if ((int) $job['operationParams']['workspaceId'] === $workspace['id']) {
                    $actualJobId = $job;
                }
            }
        }

        $this->assertArrayHasKey('metrics', $actualJobId);
        $this->assertGreaterThan(0, $actualJobId['metrics']['outBytes']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $this->assertEquals(5, $backend->countRows('languages'));
        $this->assertEquals(15, $backend->countRows('rates'));
    }

    public function testOutBytesMetricsWithLoadWorkspaceWithSeconds(): void
    {
        $this->markTestSkipped('missing incremental import');
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace([], true);

        // Create a table of sample data
        $table1Id = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
        );

        $table2Id = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'users',
            new CsvFile(__DIR__ . '/../../_data/users.csv'),
        );

        sleep(35);
        $startTime = time();

        $importCsv = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $this->_client->writeTableAsync($table1Id, $importCsv, [
            'incremental' => true,
        ]);

        $workspaces->loadWorkspaceData($workspace['id'], [
            'input' => [
                [
                    'source' => $table1Id,
                    'destination' => 'languages',
                    'seconds' => floor(time() - $startTime) + 30,
                ],
                [
                    'source' => $table2Id,
                    'destination' => 'users',
                    'seconds' => floor(time() - $startTime) + 30,
                ],
            ],
        ]);

        $actualJobId = null;
        foreach ($this->_client->listJobs() as $job) {
            if ($job['operationName'] === 'workspaceLoad') {
                if ((int) $job['operationParams']['workspaceId'] === $workspace['id']) {
                    $actualJobId = $job;
                }
            }
        }

        $this->assertArrayHasKey('metrics', $actualJobId);
        $this->assertEquals(1024, $actualJobId['metrics']['outBytes']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $this->assertEquals(5, $backend->countRows('languages'));
        $this->assertEquals(0, $backend->countRows('users'));
    }

    public function dataTypesDiffDefinitions()
    {
        return [
            [
                'rates',
                [
                    [
                        'source' =>  'Date',
                        'type' => 'TIMESTAMP',
                    ],
                ],
                [
                    [
                        'source' =>  'Date',
                        'type' => 'TIMESTAMP WITH LOCAL TIME ZONE',
                    ],
                ],
                true,
            ],
            [
                'rates',
                [
                    [
                        'source' =>  'Date',
                        'type' => 'DATE',
                    ],
                ],
                [
                    [
                        'source' =>  'Date',
                        'type' => 'TIMESTAMP',
                    ],
                ],
                true,
            ],
            [
                'languages',
                [
                    [
                        'source' =>  'id',
                        'type' => 'SMALLINT',
                    ],
                ],
                [
                    [
                        'source' =>  'id',
                        'type' => 'NUMBER',
                    ],
                ],
                true,
            ],
        ];
    }
}
