<?php

namespace Keboola\Test\Backend\Bigquery;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;
use PDO;

class ImportTreatValuesAsNullTest extends ParallelWorkspacesTestCase
{
    /**
     * @dataProvider treatValuesAsNullData
     */
    public function testImportTreatValuesAsNull(
        string $importedFile,
        array $expectedData,
        ?array $treatValuesAsNull = null
    ): void {
        $this->allowTestForBackendsOnly([
            self::BACKEND_BIGQUERY,
        ]);
        $tableName = 'testImportTreatValuesAsNull';
        $tableId = $this->createTable($tableName);

        $params = [];
        if ($treatValuesAsNull !== null) {
            $params = ['treatValuesAsNull' => $treatValuesAsNull,];
        }
        $this->_client->writeTableAsync($tableId, new CsvFile($importedFile), $params);

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace([], true);
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => $tableName,
                    'useView' => true,
                ],
            ],
            'preserve' => false,
        ];
        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $data = $backend->fetchAll($tableName, PDO::FETCH_NUM, 'Id');
        $data = array_map(function ($row) {
            unset($row[4]); // remove timestamp
            return $row;
        }, $data);

        $this->assertArraySameSorted($expectedData, $data, 0);
        $workspaces->deleteWorkspace($workspace['id']);
    }

    /**
     * @return Generator<string, array{importedFile: string, expectedData: array{array{mixed}}, treatValuesAsNull?: array<string>|null}>
     */
    public function treatValuesAsNullData(): Generator
    {
        yield 'empty-array' => [
            'importedFile' => __DIR__ . '/../../_data/languages-empty-string.csv',
            'expectedData' => [
                0 => [
                    0 => 30,
                    1 => 'armenia',
                    2 => '',
                    3 => 'b',
                ],
                1 => [
                    0 => 31,
                    1 => 'belarus',
                    2 => '',
                    3 => 'b',
                ],
                2 => [
                    0 => 32,
                    1 => 'malta',
                    2 => 'a',
                    3 => 'b',
                ],
            ],
            'treatValuesAsNull' => [],
        ];
        yield 'default' => [
            'importedFile' => __DIR__ . '/../../_data/languages-empty-string.csv',
            'expectedData' => [
                0 => [
                    0 => 30,
                    1 => 'armenia',
                    2 => null,
                    3 => 'b',
                ],
                1 => [
                    0 => 31,
                    1 => 'belarus',
                    2 => null,
                    3 => 'b',
                ],
                2 => [
                    0 => 32,
                    1 => 'malta',
                    2 => 'a',
                    3 => 'b',
                ],
            ],
        ];
        yield 'string' => [
            'importedFile' => __DIR__ . '/../../_data/languages-empty-string.csv',
            'expectedData' => [
                0 => [
                    0 => 30,
                    1 => 'armenia',
                    2 => '',
                    3 => null,
                ],
                1 => [
                    0 => 31,
                    1 => 'belarus',
                    2 => '',
                    3 => null,
                ],
                2 => [
                    0 => 32,
                    1 => 'malta',
                    2 => 'a',
                    3 => null,
                ],
            ],
            'treatValuesAsNull' => ['b'],
        ];
    }

    private function createTable(string $tableName): string
    {
        $bucketId = $this->getTestBucketId();
        $data = [
            'name' => $tableName,
            'primaryKeysNames' => ['Id'],
            'columns' => [
                [
                    'name' => 'Id',
                    'definition' => [
                        'type' => 'INT64',
                        'nullable' => false,
                    ],
                ],
                [
                    'name' => 'Name',
                    'definition' => [
                        'type' => 'STRING',
                    ],
                ],
                [
                    'name' => 'iso',
                    'definition' => [
                        'type' => 'STRING',
                        'nullable' => true,
                    ],
                ],
                [
                    'name' => 'Something',
                    'definition' => [
                        'type' => 'STRING',
                        'nullable' => true,
                    ],
                ],
            ],
        ];

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        return $this->_client->createTableDefinition($bucketId, $data);
    }
}
