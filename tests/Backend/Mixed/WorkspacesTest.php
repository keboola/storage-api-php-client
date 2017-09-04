<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Mixed;

use Keboola\StorageApi\ClientException;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\WorkspacesTestCase;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class WorkspacesTest extends WorkspacesTestCase
{
    public function testCreateWorkspaceForMysqlBackendShouldNotBeAllowed()
    {
        $workspaces = new Workspaces($this->_client);

        try {
            $workspaces->createWorkspace([
                'backend' => self::BACKEND_MYSQL,
            ]);
            $this->fail('Mysql workspace should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('backend.notSupported', $e->getStringCode());
        }
    }

    /**
     * @dataProvider  workspaceBackendData
     * @param $backend
     * @param $dataTypesDefinition
     */
    public function testCreateWorkspaceParam($backend, $dataTypesDefinition)
    {
        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace([
            'backend' => $backend,
        ]);
        $this->assertEquals($backend, $workspace['connection']['backend']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $backend->createTable("mytable", $dataTypesDefinition);
    }

    /**
     * @dataProvider workspaceMixedBackendData
     * @param $backend
     */
    public function testMixedBackendWorkspaceLoad($backend, $bucketBackend)
    {
        if ($this->_client->bucketExists("out.c-mixed-test-" . $bucketBackend)) {
            $this->_client->dropBucket(
                "out.c-mixed-test-{$bucketBackend}",
                [
                    'force' => true,
                ]
            );
        }

        if ($this->_client->bucketExists("in.c-mixed-test-" . $bucketBackend)) {
            $this->_client->dropBucket(
                "in.c-mixed-test-{$bucketBackend}",
                [
                    'force' => true,
                ]
            );
        }

        $bucketId = $this->_client->createBucket("mixed-test-{$bucketBackend}", "in", "", $bucketBackend);
        $outBucketId =  $this->_client->createBucket("mixed-test-{$bucketBackend}", "out", "", $bucketBackend);

        //setup test table
        $table1Id = $this->_client->createTable(
            $bucketId,
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            [
                'primaryKey' => 'id',
            ]
        );

        $table2Id = $this->_client->createAliasTable(
            $outBucketId,
            $table1Id,
            'Languages'
        );

        $table3Id = $this->_client->createAliasTable(
            $outBucketId,
            $table1Id,
            'LanguagesOneColumn',
            [
                'aliasColumns' => [
                    'id',
                ],
            ]
        );

        $table4Id = $this->_client->createAliasTable(
            $outBucketId,
            $table1Id,
            'LanguagesFiltered',
            [
                'aliasColumns' => [
                    'id',
                ],
                'aliasFilter' => [
                    'column' => 'id',
                    'values' => ['1']
                ]
            ]
        );

        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace([
            'backend' => $backend,
        ]);

        $options = [
            "input" => [
                [
                    "source" => $table1Id,
                    "destination" => "{$bucketBackend}_Languages"
                ],
                [
                    "source" => $table2Id,
                    "destination" => "languagesAlias",
                ],
                [
                    "source" => $table3Id,
                    "destination" => "languagesOneColumn",
                ],
                [
                    "source" => $table4Id,
                    "destination" => "languagesFiltered",
                ],
                [
                    "source" => $table4Id,
                    "destination" => "languagesFilteredRenamed",
                ]
            ]
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $workspaceBackend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        // test first table
        $data = $workspaceBackend->fetchAll("{$bucketBackend}_Languages", \PDO::FETCH_ASSOC);

        $this->assertArrayEqualsSorted(Client::parseCsv(file_get_contents(__DIR__ . '/../../_data/languages.csv'), true, ",", '"'), $data, 'id');

        // second table
        $data = $workspaceBackend->fetchAll("languagesAlias", \PDO::FETCH_ASSOC);
        $this->assertArrayEqualsSorted(Client::parseCsv(file_get_contents(__DIR__ . '/../../_data/languages.csv'), true, ",", '"'), $data, 'id');

        // third table
        $data = $workspaceBackend->fetchAll("languagesOneColumn", \PDO::FETCH_ASSOC);

        $this->assertCount(1, $data[0], 'there should be one column');
        $this->assertArrayHasKey('id', $data[0]);
        $expected = Client::parseCsv(file_get_contents(__DIR__ . '/../../_data/languages.csv'), true, ",", '"');
        $expected = array_map(function ($row) {
            return [
                'id' => $row['id'],
            ];
        }, $expected);
        $this->assertArrayEqualsSorted($expected, $data, 'id');

        // fourth table
        $data = $workspaceBackend->fetchAll("languagesFiltered", \PDO::FETCH_ASSOC);
        $this->assertCount(1, $data[0], 'there should be one column');
        $this->assertArrayHasKey('id', $data[0]);
        $this->assertEquals('1', $data[0]['id']);

        // fifth table
        $data = $workspaceBackend->fetchAll("languagesFilteredRenamed", \PDO::FETCH_ASSOC);
        $this->assertCount(1, $data[0], 'there should be one column');
        $this->assertArrayHasKey('id', $data[0]);
        $this->assertEquals('1', $data[0]['id']);
    }


    /**
     * @dataProvider loadToRedshiftDataTypes
     * @param $dataTypesDefinition
     */
    public function testDataTypesLoadToRedshift($dataTypesDefinition)
    {

        $bucketBackend = self::BACKEND_MYSQL;

        if ($this->_client->bucketExists("out.c-mixed-test-" . $bucketBackend)) {
            $this->_client->dropBucket(
                "out.c-mixed-test-{$bucketBackend}",
                [
                    'force' => true,
                ]
            );
        }

        if ($this->_client->bucketExists("in.c-mixed-test-" . $bucketBackend)) {
            $this->_client->dropBucket("in.c-mixed-test-{$bucketBackend}", [
                'force' => true,
            ]);
        }
        $bucketId = $this->_client->createBucket("mixed-test-{$bucketBackend}", "in", "", $bucketBackend);

        //setup test table
        $this->_client->createTable(
            $bucketId,
            'dates',
            new CsvFile(__DIR__ . '/../../_data/dates.csv')
        );

        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace([
            'backend' => self::BACKEND_REDSHIFT,
        ]);

        $options = [
            "input" => [
                [
                    "source" => "in.c-mixed-test-{$bucketBackend}.dates",
                    "destination" => "dates",
                    "datatypes" => $dataTypesDefinition
                ]
            ]
        ];

        // exception should not be thrown, date conversion should be applied
        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $wsBackend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $data = $wsBackend->fetchAll("dates", \PDO::FETCH_ASSOC);
        $this->assertCount(3, $data);
    }

    /**
     * @dataProvider workspaceMixedAndSameBackendDataWithDataTypes
     * @param $workspaceBackend
     * @param $sourceBackend
     * @param $dataTypesDefinition
     */
    public function testLoadUserError($workspaceBackend, $sourceBackend, $dataTypesDefinition)
    {
        if ($this->_client->bucketExists("in.c-mixed-test-" . $sourceBackend)) {
            $this->_client->dropBucket("in.c-mixed-test-{$sourceBackend}", [
                'force' => true,
            ]);
        }
        $bucketId = $this->_client->createBucket("mixed-test-{$sourceBackend}", "in", "", $sourceBackend);
        $sourceTableId = $this->_client->createTable(
            $bucketId,
            'transactions',
            new CsvFile(__DIR__ . '/../../_data/transactions.csv')
        );

        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace([
            'backend' => $workspaceBackend,
        ]);

        $options = [
            "input" => [
                [
                    "source" => $sourceTableId,
                    "destination" => "transactions",
                    "datatypes" => $dataTypesDefinition,
                ],
            ],
        ];

        // exception should be thrown, as quantity has empty value '' and snflk will complain.
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
        } catch (ClientException $e) {
            $this->assertEquals($e->getStringCode(), "workspace.tableLoad");
        }
    }

    /**
     * @dataProvider workspaceMixedAndSameBackendData
     * @param $workspaceBackend
     * @param $sourceBackend
     */
    public function testLoadWorkspaceExtendedDataTypesNullify($workspaceBackend, $sourceBackend)
    {
        if ($this->_client->bucketExists("in.c-mixed-test-" . $sourceBackend)) {
            $this->_client->dropBucket("in.c-mixed-test-{$sourceBackend}", [
                'force' => true,
            ]);
        }
        $bucketId = $this->_client->createBucket("mixed-test-{$sourceBackend}", "in", "", $sourceBackend);
        $sourceTableId = $this->_client->createTable(
            $bucketId,
            'transactions',
            new CsvFile(__DIR__ . '/../../_data/transactions-nullify.csv')
        );

        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace([
            'backend' => $workspaceBackend,
        ]);

        $dataType = $workspaceBackend === self::BACKEND_SNOWFLAKE ? 'NUMBER' : 'INTEGER';
        $options = [
            "input" => [
                [
                    "source" => $sourceTableId,
                    "destination" => "transactions",
                    "datatypes" => [
                        [
                            'column' => 'item',
                            'type' => 'VARCHAR',
                            'convertEmptyValuesToNull' => true
                        ],
                        [
                            'column' => 'quantity',
                            'type' => $dataType,
                            'convertEmptyValuesToNull' => true
                        ]
                    ],
                ],
            ],
        ];
        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $workspaceBackendConnection = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $data = $workspaceBackendConnection->fetchAll("transactions", \PDO::FETCH_ASSOC);
        $this->assertArrayHasKey('quantity', $data[0]);
        $this->assertArrayHasKey('item', $data[0]);
        $this->assertEquals(null, $data[0]['quantity']);
        $this->assertEquals(null, $data[0]['item']);
    }

    /**
     * @dataProvider workspaceMixedBackendData
     * @param $backend
     */
    public function testLoadIncremental($backend, $bucketBackend)
    {
        if ($this->_client->bucketExists("in.c-mixed-test-" . $bucketBackend)) {
            $this->_client->dropBucket(
                "in.c-mixed-test-{$bucketBackend}",
                [
                    'force' => true,
                ]
            );
        }

        $bucketId = $this->_client->createBucket("mixed-test-{$bucketBackend}", "in", "", $bucketBackend);

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace(["backend" => $backend]);
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

    /**
     * @dataProvider workspaceMixedBackendData
     * @param $backend
     */
    public function testLoadIncrementalNullable($backend, $bucketBackend)
    {
        if ($this->_client->bucketExists("in.c-mixed-test-" . $bucketBackend)) {
            $this->_client->dropBucket(
                "in.c-mixed-test-{$bucketBackend}",
                [
                    'force' => true,
                ]
            );
        }

        $bucketId = $this->_client->createBucket("mixed-test-{$bucketBackend}", "in", "", $bucketBackend);

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace(["backend" => $backend]);
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
                            'column' => 'name',
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
        // lower case keys - Redshift issue
        $rows = array_map(function ($row) {
            return array_change_key_case($row, CASE_LOWER);
        }, $rows);

        foreach ($rows as $row) {
            $this->assertArrayHasKey('state', $row);
            $this->assertArrayHasKey('id', $row);

            if (in_array($row['id'], ["0", "11", "24"])) {
                $this->assertNull($row['state']);
            }
        }
    }

    /**
     * @dataProvider workspaceMixedBackendData
     * @param $backend
     */
    public function testLoadIncrementalNotNullable($backend, $bucketBackend)
    {
        if ($this->_client->bucketExists("in.c-mixed-test-" . $bucketBackend)) {
            $this->_client->dropBucket(
                "in.c-mixed-test-{$bucketBackend}",
                [
                    'force' => true,
                ]
            );
        }

        $bucketId = $this->_client->createBucket("mixed-test-{$bucketBackend}", "in", "", $bucketBackend);

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace(["backend" => $backend]);
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

    public function workspaceBackendData()
    {
        return [
            [self::BACKEND_SNOWFLAKE, ["amount" => "NUMBER"]],
            [self::BACKEND_REDSHIFT, ["amount" => "INT"]],
        ];
    }

    public function workspaceMixedAndSameBackendDataWithDataTypes()
    {
        $simpleDataTypesDefinitionSnowflake = ["price" => "VARCHAR", "quantity" => "NUMBER"];
        $simpleDataTypesDefinitionRedshift = ["price" => "VARCHAR", "quantity" => "INTEGER"];
        $extendedDataTypesDefinitionSnowflake = [["column" => "price", "type" => "VARCHAR"], ["column" => "quantity", "type" => "NUMBER"]];
        $extendedDataTypesDefinitionRedshift = [["column" => "price", "type" => "VARCHAR"], ["column" => "quantity", "type" => "INTEGER"]];
        return [
            [self::BACKEND_SNOWFLAKE, self::BACKEND_SNOWFLAKE, $simpleDataTypesDefinitionSnowflake],
            [self::BACKEND_SNOWFLAKE, self::BACKEND_REDSHIFT, $simpleDataTypesDefinitionSnowflake],
            [self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE, $simpleDataTypesDefinitionRedshift],
            [self::BACKEND_REDSHIFT, self::BACKEND_REDSHIFT, $simpleDataTypesDefinitionRedshift],
            [self::BACKEND_SNOWFLAKE, self::BACKEND_SNOWFLAKE, $extendedDataTypesDefinitionSnowflake],
            [self::BACKEND_SNOWFLAKE, self::BACKEND_REDSHIFT, $extendedDataTypesDefinitionSnowflake],
            [self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE, $extendedDataTypesDefinitionRedshift],
            [self::BACKEND_REDSHIFT, self::BACKEND_REDSHIFT, $extendedDataTypesDefinitionRedshift],

        ];
    }

    public function workspaceMixedAndSameBackendData()
    {
        return [
            [self::BACKEND_SNOWFLAKE, self::BACKEND_SNOWFLAKE],
            [self::BACKEND_SNOWFLAKE, self::BACKEND_REDSHIFT],
            [self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE],
            [self::BACKEND_REDSHIFT, self::BACKEND_REDSHIFT],
        ];
    }

    public function workspaceMixedBackendData()
    {
        return [
            [self::BACKEND_SNOWFLAKE, self::BACKEND_REDSHIFT],
            [self::BACKEND_SNOWFLAKE, self::BACKEND_MYSQL],
            [self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE],
            [self::BACKEND_REDSHIFT, self::BACKEND_MYSQL],
        ];
    }

    public function loadToRedshiftDataTypes()
    {
        return [
            [['valid_from' => "TIMESTAMP"]],
            [[['column' => 'valid_from', 'type' => "TIMESTAMP"]]]
        ];
    }
}
