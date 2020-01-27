<?php

namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Csv\CsvFile;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class WorkspacesRedshiftTest extends WorkspacesTestCase
{


    public function testCreateNotSupportedBackend()
    {
        $workspaces = new Workspaces($this->_client);
        try {
            $workspaces->createWorkspace(["backend" => "snowflake"]);
            $this->fail("should not be able to create WS for unsupported backend");
        } catch (ClientException $e) {
            $this->assertEquals($e->getStringCode(), "workspace.backendNotSupported");
        }
    }

    /**
     * @dataProvider columnCompressionDataTypesDefinitions
     * @param $columnsDefinition
     */
    public function testColumnCompression($columnsDefinition)
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
                    "destination" => "languages-rs",
                    "columns" => $columnsDefinition
                ]
            ]
        ]);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $table = $backend->describeTableColumns('languages-rs');

        $this->assertEquals("varchar", $table['id']['DATA_TYPE']);
        $this->assertEquals(50, $table['id']['LENGTH']);
        $this->assertEquals("lzo", $table['id']['COMPRESSION']);

        $this->assertEquals("varchar", $table['name']['DATA_TYPE']);
        $this->assertEquals(255, $table['name']['LENGTH']);
        $this->assertEquals("bytedict", $table['name']['COMPRESSION']);
    }

    public function testLoadedSortKey()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
        $db = $this->getDbConnection($workspace['connection']);

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
                    "destination" => "languages-rs",
                    "sortKey" => "name"
                ]
            ]
        ]);

        $statement = $db->prepare("SELECT \"column\", sortkey FROM pg_table_def WHERE schemaname = ? AND tablename = ? AND \"column\" = ?;");
        $statement->execute([$workspace['connection']['schema'], "languages-rs", "name"]);

        $row = $statement->fetch();

        $this->assertEquals(1, (int)$row['sortkey']);
    }

    /**
     * @dataProvider  distTypeData
     * @param $dist
     */
    public function testLoadedDist($dist)
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
        $db = $this->getDbConnection($workspace['connection']);

        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages-rs',
            new CsvFile($importFile)
        );
        $mapping = [
            "source" => $tableId,
            "destination" => "languages"
        ];
        if (is_array($dist)) {
            $mapping['distKey'] = $dist['key'];
            $mapping['distStyle'] = "key";
        } else {
            $mapping['distStyle'] = $dist;
        }
        $workspaces->loadWorkspaceData($workspace['id'], [
            "input" => [
                $mapping
            ]
        ]);

        if (is_array($dist)) {
            $statement = $db->prepare("SELECT \"column\", distkey FROM pg_table_def WHERE schemaname = ? AND tablename = ? AND \"column\" = ?;");
            $statement->execute([$workspace['connection']['schema'], "languages", "id"]);
            $row = $statement->fetch();
            $this->assertEquals(1, (int)$row['distkey']);
        }

        $statement = $db->prepare("select relname, reldiststyle from pg_class where relname = 'languages';");
        $statement->execute();
        $row = $statement->fetch();
        if (is_array($dist)) {
            $this->assertEquals(1, (int)$row['reldiststyle'], "key diststyle doesn't check out.");
        } else if ($dist === 'even') {
            $this->assertEquals(0, (int)$row['reldiststyle'], "even diststyle doesn't check out.");
        } else if ($dist === "all") {
            $this->assertEquals(8, (int)$row['reldiststyle'], "all diststyle doesn't check out.");
        }
    }

    public function testLoadDataTypesDefaults()
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
                    "destination" => "languages-rs",
                    "columns" => [
                        [
                            'source' => 'id',
                            'type' => 'int',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ]
                    ]
                ]
            ]
        ]);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $table = $backend->describeTableColumns('languages-rs');

        $this->assertEquals("int4", $table['id']['DATA_TYPE']);
        $this->assertEquals(4, $table['id']['LENGTH']);

        $this->assertEquals("varchar", $table['name']['DATA_TYPE']);
        $this->assertEquals(256, $table['name']['LENGTH']);
    }

    public function testLoadedPrimaryKeys()
    {
        $primaries = ['Paid_Search_Engine_Account', 'Date', 'Paid_Search_Campaign', 'Paid_Search_Ad_ID', 'Site__DFA'];
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
        $this->assertEquals("varchar", $cols['paid_search_engine_account']['DATA_TYPE']);
        $this->assertEquals(255, $cols['paid_search_engine_account']['LENGTH']);
        $this->assertEquals("varchar", $cols['date']['DATA_TYPE']);
        $this->assertEquals(255, $cols['date']['LENGTH']);
        $this->assertEquals("varchar", $cols['paid_search_campaign']['DATA_TYPE']);
        $this->assertEquals(255, $cols['paid_search_campaign']['LENGTH']);
        $this->assertEquals("varchar", $cols['paid_search_ad_id']['DATA_TYPE']);
        $this->assertEquals(255, $cols['paid_search_ad_id']['LENGTH']);
        $this->assertEquals("varchar", $cols['site__dfa']['DATA_TYPE']);
        $this->assertEquals(255, $cols['site__dfa']['LENGTH']);
        $this->assertEquals("varchar", $cols['advertiser_id']['DATA_TYPE']);
        $this->assertEquals(65535, $cols['advertiser_id']['LENGTH']);

        // Check that PK is NOT set if not all PK columns are present
        $mapping2 = [
            "source" => $pkTableId,
            "destination" => "languages-pk-skipped",
            "columns" => [
                [
                    'source' => 'Paid_Search_Engine_Account',
                    'type' => 'varchar',
                ],
                [
                    'source' => 'Date',
                    'type' => 'varchar',
                ],
            ] // missing PK columns
        ];
        $workspaces->loadWorkspaceData($workspace['id'], ["input" => [$mapping2]]);

        $cols = $backend->describeTableColumns("languages-pk-skipped");
        $this->assertCount(2, $cols);
        $this->assertEquals("varchar", $cols['paid_search_engine_account']['DATA_TYPE']);
        $this->assertEquals(256, $cols['paid_search_engine_account']['LENGTH']);
        $this->assertEquals("varchar", $cols['date']['DATA_TYPE']);
        $this->assertEquals(256, $cols['date']['LENGTH']);
    }

    public function distTypeData()
    {
        return [
            ["all"],
            ["even"],
            [["key" => "id"]]
        ];
    }

    public function columnCompressionDataTypesDefinitions()
    {
        return [
            [
                [
                    [
                        'source' => 'id',
                        'type' => 'varchar',
                        'length' => 50,
                    ],
                    [
                        'source' => 'name',
                        'type' => 'varchar',
                        'length' => 255,
                        'compression' => 'BYTEDICT',
                    ],
                ]
            ],
        ];
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
            $this->assertArrayHasKey('state', $row);
            $this->assertArrayHasKey('id', $row);

            if (in_array($row['id'], ["0", "11", "24"])) {
                $this->assertNull($row['state']);
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
                        ]
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
}
