<?php

namespace Keboola\Test\Backend\Mixed\Workspaces;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class S3WorkspacesTest extends BaseWorkSpacesTestCase
{
    public function workspaceBackendData()
    {
        return [
            [self::BACKEND_SNOWFLAKE, ['amount' => 'NUMBER']],
            [self::BACKEND_REDSHIFT, ['amount' => 'INT']],
        ];
    }

    public function workspaceMixedAndSameBackendDataWithDataTypes()
    {
        $simpleDataTypesDefinitionSnowflake = [
            [
                'source' => 'price',
                'type' => 'VARCHAR',
            ],
            [
                'source' => 'quantity',
                'type' =>'NUMBER',
            ],
        ];
        $simpleDataTypesDefinitionRedshift = [
            [
                'source' => 'price',
                'type' => 'VARCHAR',
            ],
            [
                'source' => 'quantity',
                'type' =>'INTEGER',
            ],
        ];
        return [
            [self::BACKEND_SNOWFLAKE, self::BACKEND_SNOWFLAKE, $simpleDataTypesDefinitionSnowflake],
            [self::BACKEND_SNOWFLAKE, self::BACKEND_REDSHIFT, $simpleDataTypesDefinitionSnowflake],
            [self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE, $simpleDataTypesDefinitionRedshift],
            [self::BACKEND_REDSHIFT, self::BACKEND_REDSHIFT, $simpleDataTypesDefinitionRedshift],
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
            [self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE],
        ];
    }


    /**
     * @dataProvider workspaceMixedBackendData
     * @param $backend
     */
    public function testLoadIncremental($backend, $bucketBackend): void
    {
        if ($this->_client->bucketExists('in.c-mixed-test-' . $bucketBackend)) {
            $this->_client->dropBucket(
                "in.c-mixed-test-{$bucketBackend}",
                [
                    'force' => true,
                ],
            );
        }

        $bucketId = $this->_client->createBucket("mixed-test-{$bucketBackend}", 'in', '', $bucketBackend);

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace(['backend' => $backend], true);
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../../_data/languages.csv';
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
                    'whereColumn' => 'name',
                    'whereValues' => ['czech', 'french'],
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
                    'whereColumn' => 'name',
                    'whereValues' => ['english', 'czech'],
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(3, $backend->countRows('languages'));
    }

    /**
     * @dataProvider workspaceMixedBackendData
     * @param $backend
     */
    public function testLoadIncrementalNullable($backend, $bucketBackend): void
    {
        if ($this->_client->bucketExists('in.c-mixed-test-' . $bucketBackend)) {
            $this->_client->dropBucket(
                "in.c-mixed-test-{$bucketBackend}",
                [
                    'force' => true,
                ],
            );
        }

        $bucketId = $this->_client->createBucket("mixed-test-{$bucketBackend}", 'in', '', $bucketBackend);

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace(['backend' => $backend], true);
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../../_data/languages.with-state.csv';
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
                            'source' =>  'id',
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
                            'source' =>  'State',
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
                            'source' =>  'id',
                            'type' => 'SMALLINT',
                            'nullable' => false,
                        ],
                        [
                            'source' =>  'name',
                            'type' => 'VARCHAR',
                            'length' => '50',
                            'nullable' => false,
                        ],
                        [
                            'source' =>  'State',
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

            if (in_array($row['id'], ['0', '11', '24'])) {
                $this->assertNull($row['state']);
            }
        }
    }

    /**
     * @dataProvider workspaceMixedBackendData
     * @param $backend
     */
    public function testLoadIncrementalNotNullable($backend, $bucketBackend): void
    {
        if ($this->_client->bucketExists('in.c-mixed-test-' . $bucketBackend)) {
            $this->_client->dropBucket(
                "in.c-mixed-test-{$bucketBackend}",
                [
                    'force' => true,
                ],
            );
        }

        $bucketId = $this->_client->createBucket("mixed-test-{$bucketBackend}", 'in', '', $bucketBackend);

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace(['backend' => $backend], true);
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../../_data/languages.with-state.csv';
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
                            'source' =>  'id',
                            'type' => 'SMALLINT',
                            'nullable' => false,
                        ],
                        [
                            'source' =>  'name',
                            'type' => 'VARCHAR',
                            'length' => '50',
                            'nullable' => false,
                        ],
                        [
                            'source' =>  'State',
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
                            'source' =>  'id',
                            'type' => 'SMALLINT',
                            'nullable' => false,
                        ],
                        [
                            'source' =>  'name',
                            'type' => 'VARCHAR',
                            'length' => '50',
                            'nullable' => false,
                        ],
                        [
                            'source' =>  'State',
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

    public function testDataTypesLoadToRedshift(): void
    {

        $bucketBackend = self::BACKEND_SNOWFLAKE;

        if ($this->_client->bucketExists('out.c-mixed-test-' . $bucketBackend)) {
            $this->_client->dropBucket(
                "out.c-mixed-test-{$bucketBackend}",
                [
                    'force' => true,
                ],
            );
        }

        if ($this->_client->bucketExists('in.c-mixed-test-' . $bucketBackend)) {
            $this->_client->dropBucket("in.c-mixed-test-{$bucketBackend}", [
                'force' => true,
                            ]);
        }
        $bucketId = $this->_client->createBucket("mixed-test-{$bucketBackend}", 'in', '', $bucketBackend);

        //setup test table
        $this->_client->createTableAsync(
            $bucketId,
            'dates',
            new CsvFile(__DIR__ . '/../../../_data/dates.csv'),
        );

        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace(
            [
                'backend' => self::BACKEND_REDSHIFT,
            ],
            true,
        );

        $options = [
            'input' => [
                [
                    'source' => "in.c-mixed-test-{$bucketBackend}.dates",
                    'destination' => 'dates',
                    'columns' => [
                        [
                            'source' => 'valid_from',
                            'type' => 'TIMESTAMP',
                        ],
                    ],
                ],
            ],
        ];

        // exception should not be thrown, date conversion should be applied
        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $wsBackend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $data = $wsBackend->fetchAll('dates', \PDO::FETCH_ASSOC);
        $this->assertCount(3, $data);
    }

    /**
     * @dataProvider workspaceMixedBackendData
     * @param $backend
     */
    public function testMixedBackendWorkspaceLoad($backend, $bucketBackend): void
    {
        if ($this->_client->bucketExists('out.c-mixed-test-' . $bucketBackend)) {
            $this->_client->dropBucket(
                "out.c-mixed-test-{$bucketBackend}",
                [
                    'force' => true,
                ],
            );
        }

        if ($this->_client->bucketExists('in.c-mixed-test-' . $bucketBackend)) {
            $this->_client->dropBucket(
                "in.c-mixed-test-{$bucketBackend}",
                [
                    'force' => true,
                ],
            );
        }

        $bucketId = $this->_client->createBucket("mixed-test-{$bucketBackend}", 'in', '', $bucketBackend);
        $outBucketId =  $this->_client->createBucket("mixed-test-{$bucketBackend}", 'out', '', $bucketBackend);

        //setup test table
        $table1Id = $this->_client->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile(__DIR__ . '/../../../_data/languages.csv'),
            [
                'primaryKey' => 'id',
            ],
        );

        $table2Id = $this->_client->createAliasTable(
            $outBucketId,
            $table1Id,
            'Languages',
        );

        $table3Id = $this->_client->createAliasTable(
            $outBucketId,
            $table1Id,
            'LanguagesOneColumn',
            [
                'aliasColumns' => [
                    'id',
                ],
            ],
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
                    'values' => ['1'],
                ],
            ],
        );

        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace(
            [
                'backend' => $backend,
            ],
            true,
        );

        $options = [
            'input' => [
                [
                    'source' => $table1Id,
                    'destination' => "{$bucketBackend}_Languages",
                ],
                [
                    'source' => $table2Id,
                    'destination' => 'languagesAlias',
                ],
                [
                    'source' => $table3Id,
                    'destination' => 'languagesOneColumn',
                ],
                [
                    'source' => $table4Id,
                    'destination' => 'languagesFiltered',
                ],
                [
                    'source' => $table4Id,
                    'destination' => 'languagesFilteredRenamed',
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $workspaceBackend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        // test first table
        $data = $workspaceBackend->fetchAll("{$bucketBackend}_Languages", \PDO::FETCH_ASSOC);

        $this->assertArrayEqualsSorted(Client::parseCsv(file_get_contents(__DIR__ . '/../../../_data/languages.csv'), true, ',', '"'), $data, 'id');

        // second table
        $data = $workspaceBackend->fetchAll('languagesAlias', \PDO::FETCH_ASSOC);
        $this->assertArrayEqualsSorted(Client::parseCsv(file_get_contents(__DIR__ . '/../../../_data/languages.csv'), true, ',', '"'), $data, 'id');

        // third table
        $data = $workspaceBackend->fetchAll('languagesOneColumn', \PDO::FETCH_ASSOC);

        $this->assertCount(1, $data[0], 'there should be one column');
        $this->assertArrayHasKey('id', $data[0]);
        $expected = Client::parseCsv(file_get_contents(__DIR__ . '/../../../_data/languages.csv'), true, ',', '"');
        $expected = array_map(function ($row) {
            return [
                'id' => $row['id'],
            ];
        }, $expected);
        $this->assertArrayEqualsSorted($expected, $data, 'id');

        // fourth table
        $data = $workspaceBackend->fetchAll('languagesFiltered', \PDO::FETCH_ASSOC);
        $this->assertCount(1, $data[0], 'there should be one column');
        $this->assertArrayHasKey('id', $data[0]);
        $this->assertEquals('1', $data[0]['id']);

        // fifth table
        $data = $workspaceBackend->fetchAll('languagesFilteredRenamed', \PDO::FETCH_ASSOC);
        $this->assertCount(1, $data[0], 'there should be one column');
        $this->assertArrayHasKey('id', $data[0]);
        $this->assertEquals('1', $data[0]['id']);
    }
}
