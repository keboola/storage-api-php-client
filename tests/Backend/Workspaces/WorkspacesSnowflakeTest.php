<?php

namespace Keboola\Test\Backend\Workspaces;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class WorkspacesSnowflakeTest extends ParallelWorkspacesTestCase
{
    use WorkspaceConnectionTrait;

    public function testCreateNotSupportedBackend()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        try {
            $workspaces->createWorkspace(['backend' => self::BACKEND_REDSHIFT]);
            $this->fail("should not be able to create WS for unsupported backend");
        } catch (ClientException $e) {
            $this->assertEquals($e->getStringCode(), "workspace.backendNotSupported");
        }
    }

    public function testLoadDataTypesDefaults()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

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
            "input" => [
                [
                    "source" => $table1Id,
                    "destination" => "languages",
                    "columns" => [
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
                    "source" => $table2Id,
                    "destination" => "rates",
                    "columns" => [
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
        $this->assertEquals(3072, $actualJobId['metrics']['outBytes']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $table = $backend->describeTableColumns('languages');

        $this->assertEquals('id', $table[0]['name']);
        $this->assertEquals("NUMBER(38,0)", $table[0]['type']);

        $this->assertEquals('name', $table[1]['name']);
        $this->assertEquals("VARCHAR(16777216)", $table[1]['type']);

        $table = $backend->describeTableColumns('rates');

        $this->assertEquals('Date', $table[0]['name']);
        $this->assertEquals("VARCHAR(16777216)", $table[0]['type']);

        $this->assertEquals('SKK', $table[1]['name']);
        $this->assertEquals("VARCHAR(16777216)", $table[1]['type']);
    }

    public function testStatementTimeout()
    {
        $workspace = $this->initTestWorkspace();

        $this->assertGreaterThan(0, $workspace['statementTimeoutSeconds']);

        $db = $this->getDbConnection($workspace['connection']);

        $timeout = $db->fetchAll('SHOW PARAMETERS LIKE \'STATEMENT_TIMEOUT_IN_SECONDS\'')[0]['value'];
        $this->assertEquals($workspace['statementTimeoutSeconds'], $timeout);
    }

    public function testClientSessionKeepAlive()
    {
        $workspace = $this->initTestWorkspace();

        $db = $this->getDbConnection($workspace['connection']);

        $isKeepAlive = $db->fetchAll(sprintf(
            'SHOW PARAMETERS LIKE \'CLIENT_SESSION_KEEP_ALIVE\' IN USER %s',
            $workspace['connection']['user']
        ))[0]['value'];
        $this->assertEquals('true', $isKeepAlive);
    }

    public function testTransientTables()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

        // Create a table of sample data
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $table1Id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages-rs',
            new CsvFile($importFile)
        );

        $table2Id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'users',
            new CsvFile(__DIR__ . '/../../_data/users.csv')
        );

        $workspaces->loadWorkspaceData($workspace['id'], [
            "input" => [
                [
                    "source" => $table1Id,
                    "destination" => "languages",
                ],
                [
                    "source" => $table2Id,
                    "destination" => "users",
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
        $this->assertEquals(3584, $actualJobId['metrics']['outBytes']);

        $db = $this->getDbConnection($workspace['connection']);

        // check if schema is transient
        $schemas = $db->fetchAll("SHOW SCHEMAS");

        $workspaceSchema = null;
        foreach ($schemas as $schema) {
            if ($schema['name'] === $workspace['connection']['schema']) {
                $workspaceSchema = $schema;
                break;
            }
        }

        $this->assertNotEmpty($workspaceSchema, 'schema not found');
        $this->assertEquals('TRANSIENT', $workspaceSchema['options']);

        $tables = $db->fetchAll("SHOW TABLES IN SCHEMA " . $db->quoteIdentifier($workspaceSchema['name']));
        $this->assertCount(2, $tables);

        $this->assertEquals('languages', $tables[0]['name']);
        $this->assertEquals('TRANSIENT', $tables[0]['kind']);

        $this->assertEquals('users', $tables[1]['name']);
    }

    public function testLoadedPrimaryKeys()
    {
        $primaries = ['Paid_Search_Engine_Account', 'Date', 'Paid_Search_Campaign', 'Paid_Search_Ad_ID', 'Site__DFA'];
        $pkTableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages-pk',
            new CsvFile(__DIR__ . '/../../_data/multiple-columns-pk.csv'),
            [
                'primaryKey' => implode(",", $primaries),
            ]
        );

        $mapping = [
            "source" => $pkTableId,
            "destination" => "languages-pk",
        ];

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $workspaces->loadWorkspaceData($workspace['id'], ["input" => [$mapping]]);

        $cols = $backend->describeTableColumns("languages-pk");
        $this->assertCount(6, $cols);
        $this->assertEquals("Paid_Search_Engine_Account", $cols[0]['name']);
        $this->assertEquals("VARCHAR(16777216)", $cols[0]['type']);
        $this->assertEquals("Advertiser_ID", $cols[1]['name']);
        $this->assertEquals("VARCHAR(16777216)", $cols[1]['type']);
        $this->assertEquals("Date", $cols[2]['name']);
        $this->assertEquals("VARCHAR(16777216)", $cols[2]['type']);
        $this->assertEquals("Paid_Search_Campaign", $cols[3]['name']);
        $this->assertEquals("VARCHAR(16777216)", $cols[3]['type']);
        $this->assertEquals("Paid_Search_Ad_ID", $cols[4]['name']);
        $this->assertEquals("VARCHAR(16777216)", $cols[4]['type']);
        $this->assertEquals("Site__DFA", $cols[5]['name']);
        $this->assertEquals("VARCHAR(16777216)", $cols[5]['type']);

        // Check that PK is NOT set if not all PK columns are present
        $mapping2 = [
            "source" => $pkTableId,
            "destination" => "languages-pk-skipped",
            "columns" => [
                [
                    "source" => "Paid_Search_Engine_Account",
                    "type" => "varchar",
                ],
                [
                    "source" => "Date",
                    "type" => "varchar",
                ],
            ],
        ];
        $workspaces->loadWorkspaceData($workspace['id'], ["input" => [$mapping2]]);

        $cols = $backend->describeTableColumns("languages-pk-skipped");
        $this->assertCount(2, $cols);
        $this->assertEquals("Paid_Search_Engine_Account", $cols[0]['name']);
        $this->assertEquals("VARCHAR(16777216)", $cols[0]['type']);
        $this->assertEquals("Date", $cols[1]['name']);
        $this->assertEquals("VARCHAR(16777216)", $cols[1]['type']);
    }

    public function testLoadIncremental()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();
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
        $this->assertEquals(3072, $actualJobId['metrics']['outBytes']);

        $this->assertEquals(2, $backend->countRows("languages"));
        $this->assertEquals(5, $backend->countRows("languagesDetails"));

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
        $this->assertEquals(3, $backend->countRows("languages"));
        $this->assertEquals(3, $backend->countRows("languagesDetails"));
    }

    public function testLoadIncrementalAndPreserve()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();
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
        $this->assertEquals(2, $backend->countRows("languages"));
        $this->assertEquals(5, $backend->countRows("languagesDetails"));

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

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();
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

            if (in_array($row['id'], ["0", "11", "24"])) {
                $this->assertNull($row['State']);
            }
        }
    }

    public function testLoadIncrementalNotNullable()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();
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
    public function testsIncrementalDataTypesDiff($table, $firstLoadColumns, $secondLoadColumns, $shouldFail)
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

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
        $workspaces = new Workspaces($this->workspaceSapiClient);
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
            "input" => [
                [
                    "source" => $table1Id,
                    "destination" => "languages",
                ],
                [
                    "source" => $table2Id,
                    "destination" => "rates",
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
        $this->assertEquals(17920, $actualJobId['metrics']['outBytes']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $this->assertEquals(5, $backend->countRows('languages'));
        $this->assertEquals(15, $backend->countRows('rates'));
    }

    public function testOutBytesMetricsWithLoadWorkspaceWithSeconds()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
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
        $this->_client->writeTable($table1Id, $importCsv, [
            'incremental' => true,
        ]);

        $workspaces->loadWorkspaceData($workspace['id'], [
            "input" => [
                [
                    "source" => $table1Id,
                    "destination" => "languages",
                    'seconds' => floor(time() - $startTime) + 30,
                ],
                [
                    "source" => $table2Id,
                    "destination" => "users",
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
        $this->assertEquals(1536, $actualJobId['metrics']['outBytes']);

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
                        'source' => 'Date',
                        'type' => 'DATETIME',
                        'length' => '0',
                    ],
                ],
                [
                    [
                        'source' => 'Date',
                        'type' => 'DATETIME',
                        'length' => '9',
                    ],
                ],
                true,
            ],
            [
                'rates',
                [
                    [
                        'source' => 'Date',
                        'type' => 'DATETIME',
                        'length' => '3',
                    ],
                ],
                [
                    [
                        'source' => 'Date',
                        'type' => 'TIMESTAMP_NTZ',
                        'length' => '3',
                    ],
                ],
                false,
            ],
            [
                'languages',
                [
                    [
                        'source' => 'id',
                        'type' => 'SMALLINT',
                    ],
                ],
                [
                    [
                        'source' => 'id',
                        'type' => 'NUMBER',
                    ],
                ],
                false,
            ],
            [
                'languages',
                [
                    [
                        'source' => 'id',
                        'type' => 'DOUBLE',
                    ],
                ],
                [
                    [
                        'source' => 'id',
                        'type' => 'REAL',
                    ],
                ],
                false,
            ],
        ];
    }
}
