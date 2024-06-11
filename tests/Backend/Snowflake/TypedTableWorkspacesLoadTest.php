<?php

namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\InputMappingConverter;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;

class TypedTableWorkspacesLoadTest extends ParallelWorkspacesTestCase
{
    /**
     * @param string[] $primaryKeys
     */
    private function createTableLanguagesMoreColumns(array $primaryKeys = []): string
    {
        $payload = [
            'name' => 'languagesColumns',
            'columns' => [
                [
                    'name' => 'Id',
                    'definition' => [
                        'type' => 'INTEGER',
                    ],
                ],
                [
                    'name' => 'Name',
                    'definition' => [
                        'type' => 'VARCHAR',
                    ],
                ],
                [
                    'name' => 'iso',
                    'definition' => [
                        'type' => 'VARCHAR',
                    ],
                ],
                [
                    'name' => 'Something',
                    'definition' => [
                        'type' => 'VARCHAR',
                    ],
                ],
            ],
        ];
        if ($primaryKeys !== []) {
            $payload['primaryKeysNames'] = $primaryKeys;
        }
        $tableId = $this->_client->createTableDefinition(
            $this->getTestBucketId(self::STAGE_IN),
            $payload,
        );
        $this->_client->writeTableAsync($tableId, new CsvFile(__DIR__ . '/../../_data/languages-more-columns.csv'));
        return $tableId;
    }

    /**
     * @param string[] $primaryKeys
     */
    private function createTableLanguages(array $primaryKeys = []): string
    {
        $payload = [
            'name' => 'languagesColumns',
            'columns' => [
                [
                    'name' => 'id',
                    'definition' => [
                        'type' => 'INTEGER',
                    ],
                ],
                [
                    'name' => 'name',
                    'definition' => [
                        'type' => 'VARCHAR',
                    ],
                ],
            ],
        ];
        if ($primaryKeys !== []) {
            $payload['primaryKeysNames'] = $primaryKeys;
        }
        $tableId = $this->_client->createTableDefinition(
            $this->getTestBucketId(self::STAGE_IN),
            $payload,
        );
        $this->_client->writeTableAsync($tableId, new CsvFile(__DIR__ . '/../../_data/languages.csv'));
        return $tableId;
    }

    /**
     * @param string[] $primaryKeys
     */
    private function createTableUsers(array $primaryKeys = []): string
    {
        $payload = [
            'name' => 'languagesColumns',
            'columns' => [
                [
                    'name' => 'id',
                    'definition' => [
                        'type' => 'INTEGER',
                    ],
                ],
                [
                    'name' => 'name',
                    'definition' => [
                        'type' => 'VARCHAR',
                    ],
                ],
                [
                    'name' => 'city',
                    'definition' => [
                        'type' => 'VARCHAR',
                    ],
                ],
                [
                    'name' => 'sex',
                    'definition' => [
                        'type' => 'VARCHAR',
                    ],
                ],
            ],
        ];
        if ($primaryKeys !== []) {
            $payload['primaryKeysNames'] = $primaryKeys;
        }
        $tableId = $this->_client->createTableDefinition(
            $this->getTestBucketId(self::STAGE_IN),
            $payload,
        );
        $this->_client->writeTableAsync(
            $tableId,
            new CsvFile(__DIR__ . '/../../_data/users.csv'),
            [
                'treatValuesAsNull' => [],
            ],
        );
        return $tableId;
    }

    public function testWorkspaceLoadColumns(): void
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        //setup test tables
        $tableId = $this->createTableLanguagesMoreColumns();

        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languagesIso',
                    'columns' => [
                        [
                            'source' => 'Id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'iso',
                            'type' => 'varchar',
                        ],
                    ],
                ],
                [
                    'source' => $tableId,
                    'destination' => 'languagesSomething',
                    'columns' => [
                        [
                            'source' => 'Name',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'Something',
                            'type' => 'varchar',
                        ],
                    ],
                ],
            ],
        ];

        $mappingColumns = [
            array_map(
                function ($row) {
                    return $row['source'];
                },
                $options['input'][0]['columns'],
            ),
            array_map(
                function ($row) {
                    return $row['source'];
                },
                $options['input'][1]['columns'],
            ),
        ];

        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options,
        );
        $workspaces->loadWorkspaceData($workspace['id'], $options);

        // check that the tables have the appropriate columns
        $columns = $backend->getTableColumns($backend->toIdentifier('languagesIso'));
        $this->assertEquals(2, count($columns));
        $this->assertEquals(0, count(array_diff($columns, $backend->toIdentifier($mappingColumns[0]))));

        $columns = $backend->getTableColumns($backend->toIdentifier('languagesSomething'));
        $this->assertEquals(2, count($columns));
        $this->assertEquals(0, count(array_diff($columns, $backend->toIdentifier($mappingColumns[1]))));

        // test for invalid columns
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languagesIso',
                    'columns' => [
                        [
                            'source' => 'Id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'iso',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'not-a-column',
                            'type' => 'varchar',
                        ],
                    ],
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options,
        );
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('Trying to select a non existent column should fail');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.nonExistingColumns', $e->getStringCode());
        }
    }

    public function testLoadIncrementalWithColumns(): void
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $tableId = $this->createTableLanguagesMoreColumns(['Id']);

        // first load
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languagesDetails',
                    'whereColumn' => 'iso',
                    'whereValues' => ['dd', 'xx'],
                    'columns' => [
                        [
                            'source' => 'Id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'Name',
                            'type' => 'varchar',
                        ],
                    ],
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options,
        );

        $workspaces->loadWorkspaceData($workspace['id'], $options);
//        $this->assertEquals(2, $backend->countRows("languagesDetails"));

        // second load
        $options = [
            'input' => [
                [
                    'incremental' => true,
                    'source' => $tableId,
                    'destination' => 'languagesDetails',
                    'whereColumn' => 'iso',
                    'whereValues' => ['ff', 'xx'],
                    'columns' => [
                        [
                            'source' => 'Id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'Name',
                            'type' => 'varchar',
                        ],
                    ],
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options,
        );
        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(5, $backend->countRows('languagesDetails'));
    }

    public function testSecondsFilter(): void
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $tableId = $this->createTableLanguages();
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $originalFileLinesCount = (int) exec('wc -l <' . escapeshellarg($importFile));
        sleep(35);
        $startTime = time();
        $importCsv = new \Keboola\Csv\CsvFile($importFile);
        $this->_client->writeTableAsync($tableId, $importCsv, [
            'incremental' => true,
        ]);
        $this->_client->writeTableAsync($tableId, $importCsv, [
            'incremental' => true,
        ]);

        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'seconds' => floor(time() - $startTime) + 30,
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                    ],
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options,
        );
        $workspaces->loadWorkspaceData($workspace['id'], $options);
        // ok, the table should only have rows from the 2 most recent loads
        $numRows = $backend->countRows('languages');
        $this->assertEquals(2 * ($originalFileLinesCount - 1), $numRows, 'seconds parameter');
    }

    public function testRowsParameter(): void
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $tableId = $this->createTableLanguages();
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'rows' => 2,
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                    ],
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options,
        );
        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $numrows = $backend->countRows('languages');
        $this->assertEquals(2, $numrows, 'rows parameter');
    }

    /**
     * @param array<mixed> $exportOptions
     * @param array<mixed> $expectedResult
     * @dataProvider workspaceExportFiltersData
     */
    public function testWorkspaceExportFilters(array $exportOptions, array $expectedResult): void
    {
        $tableId = $this->createTableUsers();

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $options = [
            'input' => [
                array_merge([
                    'source' => $tableId,
                    'destination' => 'filter-test',
                ], $exportOptions),
            ],
        ];

        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options,
        );

        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $data = $backend->fetchAll('filter-test');

        $this->assertArrayEqualsSorted($expectedResult, $data, 0);
    }

    public function workspaceExportFiltersData(): array
    {
        return [
            // first test
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => ['PRG'],
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'sex',
                            'type' => 'varchar',
                        ],
                    ],
                ],
                [
                    [
                        '1',
                        'martin',
                        'male',
                    ],
                    [
                        '2',
                        'klara',
                        'female',
                    ],
                ],
            ],
            // first test with defined operator
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => ['PRG'],
                    'whereOperator' => 'eq',
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'city',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'sex',
                            'type' => 'varchar',
                        ],
                    ],
                ],
                [
                    [
                        '1',
                        'martin',
                        'PRG',
                        'male',
                    ],
                    [
                        '2',
                        'klara',
                        'PRG',
                        'female',
                    ],
                ],
            ],
            // second test
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => ['PRG', 'VAN'],
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'city',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'sex',
                            'type' => 'varchar',
                        ],
                    ],
                ],
                [
                    [
                        '1',
                        'martin',
                        'PRG',
                        'male',
                    ],
                    [
                        '2',
                        'klara',
                        'PRG',
                        'female',
                    ],
                    [
                        '3',
                        'ondra',
                        'VAN',
                        'male',
                    ],
                ],
            ],
            // third test
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => ['PRG'],
                    'whereOperator' => 'ne',
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'city',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'sex',
                            'type' => 'varchar',
                        ],
                    ],
                ],
                [
                    [
                        '5',
                        'hidden',
                        '',
                        'male',
                    ],
                    [
                        '4',
                        'miro',
                        'BRA',
                        'male',
                    ],
                    [
                        '3',
                        'ondra',
                        'VAN',
                        'male',
                    ],
                ],
            ],
            // fourth test
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => ['PRG', 'VAN'],
                    'whereOperator' => 'ne',
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'city',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'sex',
                            'type' => 'varchar',
                        ],
                    ],
                ],
                [
                    [
                        '4',
                        'miro',
                        'BRA',
                        'male',
                    ],
                    [
                        '5',
                        'hidden',
                        '',
                        'male',
                    ],
                ],
            ],
            // fifth test
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => [''],
                    'whereOperator' => 'eq',
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'city',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'sex',
                            'type' => 'varchar',
                        ],
                    ],
                ],
                [
                    [
                        '5',
                        'hidden',
                        '',
                        'male',
                    ],
                ],
            ],
            // sixth test
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => [''],
                    'whereOperator' => 'ne',
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'city',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'sex',
                            'type' => 'varchar',
                        ],
                    ],
                ],
                [
                    [
                        '4',
                        'miro',
                        'BRA',
                        'male',
                    ],
                    [
                        '1',
                        'martin',
                        'PRG',
                        'male',
                    ],
                    [
                        '2',
                        'klara',
                        'PRG',
                        'female',
                    ],
                    [
                        '3',
                        'ondra',
                        'VAN',
                        'male',
                    ],
                ],
            ],
        ];
    }
}
