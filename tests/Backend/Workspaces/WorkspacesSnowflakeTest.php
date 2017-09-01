<?php

namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Csv\CsvFile;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class WorkspacesSnowflakeTest extends WorkspacesTestCase
{

    public function testCreateNotSupportedBackend()
    {
        $workspaces = new Workspaces($this->_client);
        try {
            $workspaces->createWorkspace(["backend" => "redshift"]);
            $this->fail("should not be able to create WS for unsupported backend");
        } catch (ClientException $e) {
            $this->assertEquals($e->getStringCode(), "workspace.backendNotSupported");
        }
    }

    public function testStatementTimeout()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        $this->assertGreaterThan(0, $workspace['statementTimeoutSeconds']);

        $db = $this->getDbConnection($workspace['connection']);

        $timeout = $db->fetchAll('SHOW PARAMETERS LIKE \'STATEMENT_TIMEOUT_IN_SECONDS\'')[0]['value'];
        $this->assertEquals($workspace['statementTimeoutSeconds'], $timeout);
    }

    public function testTransientTables()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        // Create a table of sample data
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages-rs',
            new CsvFile($importFile)
        );

        $workspaces->loadWorkspaceData($workspace['id'], [
            "input" => [
                [
                    "source" => $tableId,
                    "destination" => "languages",
                ]
            ]
        ]);

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
        $this->assertCount(1, $tables);
        
        $table = reset($tables);
        $this->assertEquals('languages', $table['name']);
        $this->assertEquals('TRANSIENT', $table['kind']);
    }


    public function testLoadedPrimaryKeys()
    {
        $primaries = ['Paid_Search_Engine_Account','Date','Paid_Search_Campaign','Paid_Search_Ad_ID','Site__DFA'];
        $pkTableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages-pk',
            new CsvFile(__DIR__ . '/../../_data/multiple-columns-pk.csv'),
            array(
                'primaryKey' => implode(",", $primaries),
            )
        );

        $mapping = [
            "source" => $pkTableId,
            "destination" => "languages-pk"
        ];

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
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
            "columns" => ['Paid_Search_Engine_Account','Date'] // missing PK columns
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

        // first load
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
        $this->assertEquals(2, $backend->countRows("languages"));

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
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(3, $backend->countRows("languages"));
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
                    'datatypes' => [
                        'id' => [
                            'column' =>  'id',
                            'type' => 'SMALLINT',
                            'nullable' => false,
                        ],
                        'name' => [
                            'column' =>  'name',
                            'type' => 'VARCHAR',
                            'length' => '50',
                            'nullable' => false,
                        ],
                        'State' => [
                            'column' =>  'State',
                            'type' => 'VARCHAR',
                            'convertEmptyValuesToNull' => true,
                            'nullable' => true,
                        ],
                    ]
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
                    'datatypes' => [
                        'id' => [
                            'column' =>  'id',
                            'type' => 'SMALLINT',
                            'nullable' => false,
                        ],
                        'name' => [
                            'column' =>  'name',
                            'type' => 'VARCHAR',
                            'length' => '50',
                            'nullable' => false,
                        ],
                        'State' => [
                            'column' =>  'State',
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
                    'datatypes' => [
                        'id' => [
                            'column' =>  'id',
                            'type' => 'SMALLINT',
                            'nullable' => false,
                        ],
                        'name' => [
                            'column' =>  'name',
                            'type' => 'VARCHAR',
                            'length' => '50',
                            'nullable' => false,
                        ],
                        'State' => [
                            'column' =>  'State',
                            'type' => 'VARCHAR',
                            'convertEmptyValuesToNull' => true,
                            'nullable' => false,
                        ],
                    ]
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
                    'datatypes' => [
                        'id' => [
                            'column' =>  'id',
                            'type' => 'SMALLINT',
                            'nullable' => false,
                        ],
                        'name' => [
                            'column' =>  'name',
                            'type' => 'VARCHAR',
                            'length' => '50',
                            'nullable' => false,
                        ],
                        'State' => [
                            'column' =>  'State',
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
    public function testsIncrementalDataTypesDiff($table, $firstLoadDataTypes, $secondLoadDataTypes, $shouldFail)
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
                    'datatypes' => $firstLoadDataTypes,
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
                    'datatypes' => $secondLoadDataTypes,
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

    public function dataTypesDiffDefinitions()
    {
        return [
            [
                'rates',
                [
                    'Date' => [
                        'column' =>  'Date',
                        'type' => 'DATETIME',
                        'length' => '0',
                    ],
                ],
                [
                    'Date' => [
                        'column' =>  'Date',
                        'type' => 'DATETIME',
                        'length' => '9',
                    ],
                ],
                true,
            ],
            [
                'rates',
                [
                    'Date' => [
                        'column' =>  'Date',
                        'type' => 'DATETIME',
                        'length' => '3',
                    ],
                ],
                [
                    'Date' => [
                        'column' =>  'Date',
                        'type' => 'TIMESTAMP_NTZ',
                        'length' => '3',
                    ],
                ],
                false,
            ],
            [
                'languages',
                [
                    'id' => [
                        'column' =>  'id',
                        'type' => 'SMALLINT',
                    ],
                ],
                [
                    'id' => [
                        'column' =>  'id',
                        'type' => 'NUMBER',
                    ],
                ],
                false,
            ],
            [
                'languages',
                [
                    'id' => [
                        'column' =>  'id',
                        'type' => 'DOUBLE',
                    ],
                ],
                [
                    'id' => [
                        'column' =>  'id',
                        'type' => 'REAL',
                    ],
                ],
                false,
            ],
        ];
    }
}
