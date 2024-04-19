<?php

namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\Csv\CsvFile;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\Test\Backend\Workspaces\Backend\SynapseWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class WorkspacesSynapseTest extends ParallelWorkspacesTestCase
{

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

    public function testTableDistributionKey(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();
        /** @var SynapseWorkspaceBackend $backend */
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile($importFile),
            [
                'primaryKey' => 'id',
                'distributionKey' => 'name',
            ],
        );
        // legacy IM
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'whereColumn' => 'name',
                    'whereValues' => ['czech', 'french'],
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $ref = $backend->getTableReflection('languages');
        // one primary keys is used as HASH key
        $this->assertEquals('HASH', $ref->getTableDistribution());
        $this->assertEquals(['name'], $ref->getTableDistributionColumnsNames());

        // new IM
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages2',
                    'whereColumn' => 'name',
                    'whereValues' => ['czech', 'french'],
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
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $ref = $backend->getTableReflection('languages2');
        // one primary keys is used as HASH key
        $this->assertEquals('HASH', $ref->getTableDistribution());
        $this->assertEquals(['name'], $ref->getTableDistributionColumnsNames());
    }

    public function testLoadDataTypesDefaults(): void
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

        // Create a table of sample data
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

        $jobs = $this->listWorkspaceJobs($workspace['id']);
        $actualJob = reset($jobs);

        $this->assertSame('workspaceLoad', $actualJob['operationName']);
        $this->assertArrayHasKey('metrics', $actualJob);
        $this->assertGreaterThan(0, $actualJob['metrics']['outBytes']);

        /** @var SynapseWorkspaceBackend $backend */
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        /** @var ColumnCollection $table */
        $table = $backend->describeTableColumns('languages');
        $table = iterator_to_array($table->getIterator());

        $this->assertEquals('id', $table[0]->getColumnName());
        $this->assertEquals('INT', $table[0]->getColumnDefinition()->getSQLDefinition());

        $this->assertEquals('name', $table[1]->getColumnName());
        $this->assertEquals('VARCHAR(8000)', $table[1]->getColumnDefinition()->getSQLDefinition());

        /** @var ColumnCollection $table */
        $table = $backend->describeTableColumns('rates');
        $table = iterator_to_array($table->getIterator());

        $this->assertEquals('Date', $table[0]->getColumnName());
        $this->assertEquals('VARCHAR(8000)', $table[0]->getColumnDefinition()->getSQLDefinition());

        $this->assertEquals('SKK', $table[1]->getColumnName());
        $this->assertEquals('VARCHAR(8000)', $table[1]->getColumnDefinition()->getSQLDefinition());

        $ref = $backend->getTableReflection('rates');
        // no PK ROUND_ROBIN is used
        $this->assertEquals('ROUND_ROBIN', $ref->getTableDistribution());
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
        /** @var SynapseWorkspaceBackend $backend */
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $workspaces->loadWorkspaceData($workspace['id'], ['input' => [$mapping]]);

        $ref = $backend->getTableReflection('languages-pk');
        // multiple PK ROUND_ROBIN is used
        $this->assertEquals('ROUND_ROBIN', $ref->getTableDistribution());

        /** @var ColumnCollection $cols */
        $cols = $backend->describeTableColumns('languages-pk');
        $cols = iterator_to_array($cols->getIterator());
        $this->assertCount(6, $cols);
        $this->assertEquals('Paid_Search_Engine_Account', $cols[0]->getColumnName());
        $this->assertEquals('NVARCHAR(4000) NOT NULL', $cols[0]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Advertiser_ID', $cols[1]->getColumnName());
        $this->assertEquals('NVARCHAR(4000)', $cols[1]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Date', $cols[2]->getColumnName());
        $this->assertEquals('NVARCHAR(4000) NOT NULL', $cols[2]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Paid_Search_Campaign', $cols[3]->getColumnName());
        $this->assertEquals('NVARCHAR(4000) NOT NULL', $cols[3]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Paid_Search_Ad_ID', $cols[4]->getColumnName());
        $this->assertEquals('NVARCHAR(4000) NOT NULL', $cols[4]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Site__DFA', $cols[5]->getColumnName());
        $this->assertEquals('NVARCHAR(4000) NOT NULL', $cols[5]->getColumnDefinition()->getSQLDefinition());

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

        /** @var ColumnCollection $cols */
        $cols = $backend->describeTableColumns('languages-pk-skipped');
        $cols = iterator_to_array($cols->getIterator());

        $this->assertCount(2, $cols);
        $this->assertEquals('Paid_Search_Engine_Account', $cols[0]->getColumnName());
        $this->assertEquals('VARCHAR(8000)', $cols[0]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Date', $cols[1]->getColumnName());
        $this->assertEquals('VARCHAR(8000)', $cols[1]->getColumnDefinition()->getSQLDefinition());
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

        $jobs = $this->listWorkspaceJobs($workspace['id']);
        $actualJob = reset($jobs);

        $this->assertSame('workspaceLoad', $actualJob['operationName']);
        $this->assertArrayHasKey('metrics', $actualJob);
        $this->assertGreaterThan(0, $actualJob['metrics']['outBytes']);

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
        $workspace = $this->initTestWorkspace();

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

        $jobs = $this->listWorkspaceJobs($workspace['id']);
        $actualJob = reset($jobs);

        $this->assertSame('workspaceLoad', $actualJob['operationName']);
        $this->assertArrayHasKey('metrics', $actualJob);
        $this->assertGreaterThan(0, $actualJob['metrics']['outBytes']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $this->assertEquals(5, $backend->countRows('languages'));
        $this->assertEquals(15, $backend->countRows('rates'));
    }

    public function testOutBytesMetricsWithLoadWorkspaceWithSeconds(): void
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

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

        $jobs = $this->listWorkspaceJobs($workspace['id']);
        $actualJob = reset($jobs);

        $this->assertSame('workspaceLoad', $actualJob['operationName']);
        $this->assertArrayHasKey('metrics', $actualJob);
        $this->assertGreaterThan(0, $actualJob['metrics']['outBytes']);

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
                        'type' => 'datetime2',
                        'length' => '2',
                    ],
                ],
                [
                    [
                        'source' =>  'Date',
                        'type' => 'datetime2',
                        'length' => '3',
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
                        'type' => 'INT',
                    ],
                ],
                true,
            ],
            [
                'languages',
                [
                    [
                        'source' =>  'id',
                        'type' => 'FLOAT',
                    ],
                ],
                [
                    [
                        'source' =>  'id',
                        'type' => 'REAL',
                    ],
                ],
                true,
            ],
        ];
    }

    public function testTableLoadAsView(): void
    {
        $currentToken = $this->_client->verifyToken();
        self::assertArrayHasKey('owner', $currentToken);
        if (!in_array('workspace-view-load', $currentToken['owner']['features'])) {
            self::fail(sprintf(
                'Project "%s" id:"%s" is missing feature "workspace-view-load"',
                $currentToken['owner']['name'],
                $currentToken['owner']['id'],
            ));
        }

        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace([], true);
        /** @var SynapseWorkspaceBackend $backend */
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile($importFile),
        );
        $fileId = $this->workspaceSapiClient->uploadFile(
            (new CsvFile($importFile))->getPathname(),
            (new FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(true)
                ->setTags(['test-file-1']),
        );

        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'useView' => true,
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $tableRef = $backend->getTableReflection('languages');
        $viewRef = $backend->getViewReflection('languages');
        // View definition should be available
        self::assertStringStartsWith('CREATE VIEW', $viewRef->getViewDefinition());
        self::assertEquals(['id', 'name', '_timestamp'], $tableRef->getColumnsNames());
        self::assertCount(5, $backend->fetchAll('languages'));

        // test if view is refreshed after column add
        $this->_client->addTableColumn($tableId, 'newGuy');
        $tableRef = $backend->getTableReflection('languages');
        self::assertEquals(['id', 'name', '_timestamp', 'newGuy'], $tableRef->getColumnsNames());
        self::assertCount(5, $backend->fetchAll('languages'));

        // test if view is refreshed after column remove
        $this->_client->deleteTableColumn($tableId, 'newGuy');
        $tableRef = $backend->getTableReflection('languages');
        self::assertEquals(['id', 'name', '_timestamp'], $tableRef->getColumnsNames());
        self::assertCount(5, $backend->fetchAll('languages'));

        // test preserve load
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'useView' => true,
                ],
            ],
            'preserve' => true,
        ];
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            self::fail('Must throw exception view exists');
        } catch (ClientException $e) {
            self::assertEquals('Table languages already exists in workspace', $e->getMessage());
        }

        // test preserve load with overwrite
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'useView' => true,
                    'overwrite' => true,
                ],
            ],
            'preserve' => true,
        ];
        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $tableRef = $backend->getTableReflection('languages');
        self::assertEquals(['id', 'name', '_timestamp'], $tableRef->getColumnsNames());
        self::assertCount(5, $backend->fetchAll('languages'));

        // test workspace is cleared load works
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'useView' => true,
                ],
            ],
        ];
        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $tableRef = $backend->getTableReflection('languages');
        self::assertEquals(['id', 'name', '_timestamp'], $tableRef->getColumnsNames());
        self::assertCount(5, $backend->fetchAll('languages'));

        // test workspace load incremental to view
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'incremental' => true,
                    'useView' => false,
                ],
            ],
            'preserve' => true,
        ];
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            self::fail('Incremental load to view cannot work.');
        } catch (ClientException $e) {
            // this is expected edge case, view has also _timestamp col
            // which is ignored when validation incremental load
            self::assertStringStartsWith('Some columns are missing in source table', $e->getMessage());
        }

        // do incremental load from file to source table
        $this->_client->writeTableAsync(
            $tableId,
            new CsvFile($importFile),
            ['incremental' => true],
        );
        // test view is still working
        $tableRef = $backend->getTableReflection('languages');
        self::assertEquals(['id', 'name', '_timestamp'], $tableRef->getColumnsNames());
        self::assertCount(10, $backend->fetchAll('languages'));

        // load data from workspace to table
        $workspace2 = $workspaces->createWorkspace([], true);
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                ],
            ],
        ];
        $workspaces->loadWorkspaceData($workspace2['id'], $options);
        $this->_client->writeTableAsyncDirect(
            $tableId,
            [
                'dataWorkspaceId' => $workspace2['id'],
                'dataObject' => 'languages',
            ],
        );
        // test view is still working
        $tableRef = $backend->getTableReflection('languages');
        self::assertEquals(['id', 'name', '_timestamp'], $tableRef->getColumnsNames());
        self::assertCount(10, $backend->fetchAll('languages'));

// skipping the following part till there is a reason to debug it
//        // load data from file workspace
//        $fileWorkspace = $workspaces->createWorkspace(
//            [
//                'backend' => 'abs',
//            ],
//            true,
//        );
//        $options = [
//            'input' => [
//                [
//                    'dataFileId' => $fileId,
//                    'destination' => 'languages',
//                ],
//            ],
//        ];
//
//        $workspaces->loadWorkspaceData($fileWorkspace['id'], $options);
//        $this->_client->writeTableAsyncDirect(
//            $tableId,
//            [
//                'dataWorkspaceId' => $fileWorkspace['id'],
//                'dataObject' => 'languages/',
//            ],
//        );
//        // test view is still working
//        $tableRef = $backend->getTableReflection('languages');
//        self::assertEquals(['id', 'name', '_timestamp'], $tableRef->getColumnsNames());
//        // data are not loaded right due to KBC-1418
//        $backend->fetchAll('languages');
//        //self::assertCount(5, $backend->fetchAll('languages'));
//
//        // test drop table
//        $this->_client->dropTable($tableId);
//        $schemaRef = $backend->getSchemaReflection();
//        self::assertCount(0, $schemaRef->getTablesNames());
//        self::assertCount(0, $schemaRef->getViewsNames());
    }
}
