<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\Test\Backend\CommonPart1;

use Exception;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\Test\StorageApiTestCase;

/**
 * @phpstan-import-type WorkspaceResponse from Workspaces
 */
class DeleteRowsTest extends ParallelWorkspacesTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    /**
     * @param WorkspaceBackend $backend
     * @phpstan-param WorkspaceResponse $workspaceResponse
     * @return void
     * @throws Exception
     */
    protected function initData(WorkspaceBackend $backend, array $workspaceResponse): void
    {
        switch ($workspaceResponse['connection']['backend']) {
            case 'snowflake':
                $backend->executeQuery("INSERT INTO USERS VALUES (1, 'martin');");
                $backend->executeQuery("INSERT INTO USERS VALUES (3, 'ondra');");
                break;
            case 'bigquery':
                $backend->executeQuery(sprintf("ISERT INTO %s.%s USERS VALUES (1, 'martin');", BigqueryQuote::quoteSingleIdentifier($workspaceResponse['connection']['schema']), BigqueryQuote::quoteSingleIdentifier('USERS')));
                $backend->executeQuery(sprintf("'INSERT INTO %s.%s USERS VALUES (3, 'ondra');", BigqueryQuote::quoteSingleIdentifier($workspaceResponse['connection']['schema']), BigqueryQuote::quoteSingleIdentifier('USERS')));
                break;
            default:
                throw new Exception('Unknown backend');
        }
    }

    /**
     * @dataProvider tableDeleteRowsByFiltersData
     */
    public function testTableDeleteRowsByFilter(array $filterParams, array $expectedTableContent): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));
        $tableInfo = $this->_client->getTable($tableId);
        if ($this->isExasolWithNewDeleteRows($tableInfo['bucket']['backend'], $filterParams)) {
            $this->markTestSkipped('Not supported in Exasol yet.');
        }

        $this->_client->deleteTableRows($tableId, $filterParams);

        $data = $this->_client->getTableDataPreview($tableId);

        $parsedData = Client::parseCsv($data, false);
        array_shift($parsedData); // remove header

        $this->assertArrayEqualsSorted($expectedTableContent, $parsedData, 0);
    }

    public function testTableDeleteRowsByEmptyFilterWithoutAllowTruncateShouldFail(): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        try {
            $this->_client->deleteTableRows($tableId);
            $this->fail('Exception should be thrown');
        } catch (ClientException $e) {
            $this->assertSame('No filters have been specified, which will truncate the table, but the `allowTruncate` parameter was not set.', $e->getMessage());
            $this->assertSame('storage.tables.validation.unintendedTruncation', $e->getStringCode());
        }
    }

    /**
     * @dataProvider tableDeleteRowsByFiltersData
     */
    public function testTableDeleteRowsByFilterAsQuery(array $filterParams, array $expectedTableContent): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));
        $tableInfo = $this->_client->getTable($tableId);
        if ($this->isExasolWithNewDeleteRows($tableInfo['bucket']['backend'], $filterParams)) {
            $this->markTestSkipped('Not supported in Exasol yet.');
        }
        $this->_client->deleteTableRowsAsQuery($tableId, $filterParams);

        $data = $this->_client->getTableDataPreview($tableId);

        $parsedData = Client::parseCsv($data, false);
        array_shift($parsedData); // remove header

        $this->assertArrayEqualsSorted($expectedTableContent, $parsedData, 0);
    }

    // because BQ/exa does not support valuesByTableInWorkspace yet/. Tmp fix
    private function isExasolWithNewDeleteRows(string $backendName, array $filterParams): bool
    {
        return $backendName === StorageApiTestCase::BACKEND_EXASOL &&
            array_key_exists('whereFilters', $filterParams) &&
            count($filterParams['whereFilters']) > 0 &&
            (array_key_exists('valuesByTableInWorkspace', $filterParams['whereFilters'][0]));
    }

    public function testDeleteRowsMissingValuesShouldReturnUserError(): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        try {
            $this->_client->deleteTableRows($tableId, [
                'whereColumn' => 'city',
            ]);
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertSame('validation.failed', $e->getStringCode());
            $this->assertSame("Invalid request:\n - whereColumn: \"To use \"whereColumn\" specify \"whereValues\".\"", $e->getMessage());
        }
    }

    public function testDeleteRowsOnInvalidColumn(): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        try {
            $this->_client->deleteTableRows($tableId, [
                'whereFilters' => [
                    [
                        'column' => 'notExistingColumn',
                        'values' => ['PRG'],
                    ],
                ],
            ]);
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertSame('storage.tables.columnNotExists', $e->getStringCode());
            $this->assertSame('Filter validation: Cannot filter by column "notExistingColumn", column does not exist', $e->getMessage());
        }
    }

    public function tableDeleteRowsByFiltersData(): array
    {
        $yesterday = new \DateTime('-1 day');
        $tomorrow = new \DateTime('+1 day');

        return [
            'no params' => [
                [
                    'allowTruncate' => true,
                ],
                [],
            ],
            'since yesterday - timestamp' => [
                [
                    'changedSince' => $yesterday->getTimestamp(),
                ],
                [],
            ],
            'since tomorrow - timestamp' => [
                [
                    'changedSince' => $tomorrow->getTimestamp(),
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
            'deprecated where: col = value' => [
                [
                    'whereColumn' => 'city',
                    'whereValues' => ['PRG'],
                ],
                [
                    [
                        '3',
                        'ondra',
                        'VAN',
                        'male',
                    ],
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
            'deprecated where: col != value' => [
                [
                    'whereOperator' => 'ne',
                    'whereColumn' => 'city',
                    'whereValues' => ['PRG'],
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
            'deprecated where: col in values' => [
                [
                    'whereOperator' => 'ne',
                    'whereColumn' => 'city',
                    'whereValues' => ['PRG', 'BRA'],
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
                        '4',
                        'miro',
                        'BRA',
                        'male',
                    ],
                ],
            ],
            'where filter: col = value' => [
                [
                    'whereFilters' => [
                        [
                            'column' => 'city',
                            'values' => ['PRG'],
                        ],
                    ],
                ],
                [
                    [
                        '3',
                        'ondra',
                        'VAN',
                        'male',
                    ],
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
            'where filter: multiple' => [
                [
                    'whereFilters' => [
                        [
                            'column' => 'city',
                            'values' => ['PRG', 'VAN'],
                        ],
                        [
                            'column' => 'sex',
                            'values' => ['male'],
                        ],
                    ],
                ],
                [
                    [
                        '2',
                        'klara',
                        'PRG',
                        'female',
                    ],
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
        ];
    }

    public function testDeleteByValuesInWorkspaceWithInvalidData(): void
    {
        $this->skipTestForBackend([StorageApiTestCase::BACKEND_EXASOL], 'Not supported in Exasol yet.');

        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $workspace = $this->initTestWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $backend->dropTableIfExists('USERS');
        $backend->createTable('USERS', ['ID' => 'INT', 'name' => 'STRING']);
        $this->initData($backend, $workspace);

        // test invalid table
        try {
            $this->_client->deleteTableRows($tableId, [
                'whereFilters' => [
                    [
                        'column' => 'id',
                        'valuesByTableInWorkspace' => [
                            'workspaceId' => $workspace['id'],
                            'table' => 'NOTEXISTING',
                            'column' => 'ID',
                        ],
                    ],
                ],
            ]);
            $this->fail('Should fail because table does not exist');
        } catch (ClientException $e) {
            $this->assertSame(
                sprintf(
                    'Table "NOTEXISTING" not found in schema "%s"',
                    $workspace['connection']['schema'],
                ),
                $e->getMessage(),
            );
            $this->assertSame('storage.tableNotFound', $e->getStringCode());
        }

        // test type of column (storage is type of VARCHAR)
        try {
            $this->_client->deleteTableRows($tableId, [
                'whereFilters' => [
                    [
                        'column' => 'id',
                        'valuesByTableInWorkspace' => [
                            'workspaceId' => $workspace['id'],
                            'table' => 'USERS',
                            'column' => 'ID',
                        ],
                    ],
                ],
            ]);
            $this->fail('Should fail because of invalid column type');
        } catch (ClientException $e) {
            $this->assertSame('Cannot use column "ID" to delete by. Column types do not match. Type is "NUMBER" but expected type is "VARCHAR".', $e->getMessage());
            $this->assertSame('storage.tables.invalidColumnToDeleteBy', $e->getStringCode());
        }

        // test non-existing column
        try {
            $this->_client->deleteTableRows($tableId, [
                'whereFilters' => [
                    [
                        'column' => 'id',
                        'valuesByTableInWorkspace' => [
                            'workspaceId' => $workspace['id'],
                            'table' => 'USERS',
                            'column' => 'NOTEXISTING',
                        ],
                    ],
                ],
            ]);
            $this->fail('Should fail because of column does not exist');
        } catch (ClientException $e) {
            $this->assertSame('Cannot use column "NOTEXISTING" to delete by. Column does not exist.', $e->getMessage());
            $this->assertSame('storage.tables.invalidColumnToDeleteBy', $e->getStringCode());
        }

        // test non-existing workspace
        try {
            $this->_client->deleteTableRows($tableId, [
                'whereFilters' => [
                    [
                        'column' => 'id',
                        'valuesByTableInWorkspace' => [
                            'workspaceId' => 123456, // not existing workspace
                            'table' => 'USERS',
                            'column' => 'name',
                        ],
                    ],
                ],
            ]);
            $this->fail('Should fail because of column does not exist');
        } catch (ClientException $e) {
            $this->assertSame('Workspace "123456" not found.', $e->getMessage());
            $this->assertSame('workspace.workspaceNotFound', $e->getStringCode());
        }
    }

    public function testDeleteByValuesInWorkspaceWithValidData(): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $this->skipTestForBackend([StorageApiTestCase::BACKEND_EXASOL], 'Not supported in Exasol yet.');
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $workspace = $this->initTestWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $backend->dropTableIfExists('USERS');
        $backend->createTable('USERS', ['ID' => 'STRING', 'name' => 'STRING']);

        // WS table is empty - should pass and not delete anything
        $result = $this->_client->deleteTableRows($tableId, [
            'whereFilters' => [
                [
                    'column' => 'id',
                    'valuesByTableInWorkspace' => [
                        'workspaceId' => $workspace['id'],
                        'table' => 'USERS',
                        'column' => 'ID',
                    ],
                ],
            ],
        ]);
        assert(is_array($result));
        assert(array_key_exists('deletedRows', $result));
        $this->assertEquals(0, $result['deletedRows']);

        // there is one row in WS table - should delete one row
        $backend->executeQuery("INSERT INTO USERS VALUES ('3', 'ondra');");
        $result = $this->_client->deleteTableRows($tableId, [
            'whereFilters' => [
                [
                    'column' => 'id',
                    'valuesByTableInWorkspace' => [
                        'workspaceId' => $workspace['id'],
                        'table' => 'USERS',
                        'column' => 'ID',
                    ],
                ],
            ],
        ]);
        assert(is_array($result));
        assert(array_key_exists('deletedRows', $result));
        $this->assertEquals(1, $result['deletedRows']);

        $data = $this->_client->getTableDataPreview($tableId);

        $parsedData = Client::parseCsv($data, false);
        array_shift($parsedData); // remove header

        $expectedTableContent = [
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
        ];
        $this->assertArrayEqualsSorted($expectedTableContent, $parsedData, 0);
    }
}
