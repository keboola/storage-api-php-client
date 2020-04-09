<?php

namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Csv\CsvFile;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class WorkspacesSynapseTest extends WorkspacesTestCase
{

    public function testCreateNotSupportedBackend()
    {
        $workspaces = new Workspaces($this->_client);
        try {
            $workspaces->createWorkspace(['backend' => 'redshift']);
            $this->fail('should not be able to create WS for unsupported backend');
        } catch (ClientException $e) {
            $this->assertEquals($e->getStringCode(), 'workspace.backendNotSupported');
        }
    }

    public function testLoadDataTypesDefaults()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        // Create a table of sample data
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $table1Id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($importFile)
        );

        $table2Id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'rates',
            new CsvFile(__DIR__ . '/../../_data/rates.csv')
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
                    ]
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
                    ]
                ]
            ]
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
// TODO
//        $this->assertEquals(0, $actualJobId['metrics']['outBytes']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        /** @var ColumnCollection $table */
        $table = $backend->describeTableColumns('languages');
        $table = iterator_to_array($table->getIterator());

        $this->assertEquals('id', $table[0]->getColumnName());
        $this->assertEquals('int', $table[0]->getColumnDefinition()->getSQLDefinition());

        $this->assertEquals('name', $table[1]->getColumnName());
        $this->assertEquals('varchar(8000)', $table[1]->getColumnDefinition()->getSQLDefinition());

        /** @var ColumnCollection $table */
        $table = $backend->describeTableColumns('rates');
        $table = iterator_to_array($table->getIterator());

        $this->assertEquals('Date', $table[0]->getColumnName());
        $this->assertEquals('varchar(8000)', $table[0]->getColumnDefinition()->getSQLDefinition());

        $this->assertEquals('SKK', $table[1]->getColumnName());
        $this->assertEquals('varchar(8000)', $table[1]->getColumnDefinition()->getSQLDefinition());

        $this->markTestIncomplete('TODO: metrics.outBytes does not work');
    }

    public function testLoadedPrimaryKeys()
    {
        $primaries = ['Paid_Search_Engine_Account','Date','Paid_Search_Campaign','Paid_Search_Ad_ID','Site__DFA'];
        $pkTableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages-pk',
            new CsvFile(__DIR__ . '/../../_data/multiple-columns-pk.csv'),
            array(
                'primaryKey' => implode(',', $primaries),
            )
        );

        $mapping = [
            'source' => $pkTableId,
            'destination' => 'languages-pk'
        ];

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $workspaces->loadWorkspaceData($workspace['id'], ['input' => [$mapping]]);

        /** @var ColumnCollection $cols */
        $cols = $backend->describeTableColumns('languages-pk');
        $cols = iterator_to_array($cols->getIterator());
        $this->assertCount(6, $cols);
        $this->assertEquals('Paid_Search_Engine_Account', $cols[0]->getColumnName());
        $this->assertEquals('nvarchar(4000) NOT NULL', $cols[0]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Advertiser_ID', $cols[1]->getColumnName());
        $this->assertEquals('nvarchar(4000)', $cols[1]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Date', $cols[2]->getColumnName());
        $this->assertEquals('nvarchar(4000) NOT NULL', $cols[2]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Paid_Search_Campaign', $cols[3]->getColumnName());
        $this->assertEquals('nvarchar(4000) NOT NULL', $cols[3]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Paid_Search_Ad_ID', $cols[4]->getColumnName());
        $this->assertEquals('nvarchar(4000) NOT NULL', $cols[4]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Site__DFA', $cols[5]->getColumnName());
        $this->assertEquals('nvarchar(4000) NOT NULL', $cols[5]->getColumnDefinition()->getSQLDefinition());

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
        $this->assertEquals('varchar(8000)', $cols[0]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Date', $cols[1]->getColumnName());
        $this->assertEquals('varchar(8000)', $cols[1]->getColumnDefinition()->getSQLDefinition());
    }

    public function testLoadIncremental()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);


        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $bucketId,
            'languages',
            new CsvFile($importFile),
            ['primaryKey' => 'id']
        );

        $importFile = __DIR__ . '/../../_data/languages-more-columns.csv';
        $table2Id = $this->_client->createTable(
            $bucketId,
            'languagesDetails',
            new CsvFile($importFile),
            ['primaryKey' => 'Id']
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
// TODO
//        $this->assertEquals(0, $actualJobId['metrics']['outBytes']);

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

        $this->markTestIncomplete('TODO: metrics.outBytes does not work');
    }

    public function testLoadIncrementalAndPreserve()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);


        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $bucketId,
            'languages',
            new CsvFile($importFile),
            ['primaryKey' => 'id']
        );

        $importFile = __DIR__ . '/../../_data/languages-more-columns.csv';
        $table2Id = $this->_client->createTable(
            $bucketId,
            'languagesDetails',
            new CsvFile($importFile),
            ['primaryKey' => 'Id']
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

    public function testLoadIncrementalNullable()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);


        $importFile = __DIR__ . '/../../_data/languages.with-state.csv';
        $tableId = $this->_client->createTable(
            $bucketId,
            'languages',
            new CsvFile($importFile),
            ['primaryKey' => 'id']
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
                        ]
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
                        ]
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

    public function testLoadIncrementalNotNullable()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);


        $importFile = __DIR__ . '/../../_data/languages.with-state.csv';
        $tableId = $this->_client->createTable(
            $bucketId,
            'languages',
            new CsvFile($importFile),
            ['primaryKey' => 'id']
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
                        ]
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
    public function testsIncrementalDataTypesDiff($table, $firstLoadColumns, $secondLoadColumns, $shouldFail)
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        $importFile = __DIR__ . "/../../_data/$table.csv";

        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            $table,
            new CsvFile($importFile)
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
                $this->assertContains('Different mapping between', $e->getMessage());
            }
        } else {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
        }
    }

    public function testOutBytesMetricsWithLoadWorkspaceWithRows()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        // Create a table of sample data
        $table1Id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $table2Id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'rates',
            new CsvFile(__DIR__ . '/../../_data/rates.csv')
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
                ]
            ]
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
// TODO
//        $this->assertEquals(7168, $actualJobId['metrics']['outBytes']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $this->assertEquals(5, $backend->countRows('languages'));
        $this->assertEquals(15, $backend->countRows('rates'));

        $this->markTestIncomplete('TODO: metrics.outBytes does not work');
    }

    public function testOutBytesMetricsWithLoadWorkspaceWithSeconds()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        // Create a table of sample data
        $table1Id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $table2Id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'users',
            new CsvFile(__DIR__ . '/../../_data/users.csv')
        );

        sleep(35);
        $startTime = time();

        $importCsv = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $this->_client->writeTable($table1Id, $importCsv, array(
            'incremental' => true,
        ));

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
                ]
            ]
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
// TODO
//        $this->assertEquals(1024, $actualJobId['metrics']['outBytes']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $this->assertEquals(5, $backend->countRows('languages'));
        $this->assertEquals(0, $backend->countRows('users'));

        $this->markTestIncomplete('TODO: metrics.outBytes does not work');
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
}
