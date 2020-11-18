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
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\WorkspacesTestCase;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

abstract class LegacyWorkspacesBaseCase extends WorkspacesTestCase
{
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
                        [
                            'column' =>  'id',
                            'type' => 'SMALLINT',
                            'nullable' => false,
                        ],
                        [
                            'column' =>  'name',
                            'type' => 'VARCHAR',
                            'length' => '50',
                            'nullable' => false,
                        ],
                        [
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
                        [
                            'column' =>  'id',
                            'type' => 'SMALLINT',
                            'nullable' => false,
                        ],
                        [
                            'column' =>  'name',
                            'type' => 'VARCHAR',
                            'length' => '50',
                            'nullable' => false,
                        ],
                        [
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
                        [
                            'column' =>  'id',
                            'type' => 'SMALLINT',
                            'nullable' => false,
                        ],
                        [
                            'column' => 'name',
                            'type' => 'VARCHAR',
                            'length' => '50',
                            'nullable' => false,
                        ],
                        [
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
                        [
                            'column' =>  'id',
                            'type' => 'SMALLINT',
                            'nullable' => false,
                        ],
                        [
                            'column' =>  'name',
                            'type' => 'VARCHAR',
                            'length' => '50',
                            'nullable' => false,
                        ],
                        [
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

    abstract public function workspaceMixedBackendData();

    abstract public function workspaceMixedAndSameBackendData();

    abstract public function workspaceMixedAndSameBackendDataWithDataTypes();
}
