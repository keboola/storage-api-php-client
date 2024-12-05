<?php

namespace Keboola\Test\Backend\Snowflake;

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
            self::BACKEND_SNOWFLAKE,
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

        $data = $backend->fetchAll($tableName);
        $data = array_map(function ($row) {
            unset($row[4]); // remove timestamp
            return $row;
        }, $data);

        $this->assertArraySameSorted($expectedData, $data, 0);
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
                    0 => '30',
                    1 => 'armenia with null',
                    2 => null, // col without enclosure is null by default
                    3 => 'b',
                ],
                1 => [
                    0 => '31',
                    1 => 'belarus with empty string',
                    2 => '',
                    3 => 'c',
                ],
                2 => [
                    0 => '32',
                    1 => 'malta',
                    2 => 'b',
                    3 => 'b',
                ],
            ],
            'treatValuesAsNull' => [],
        ];
        yield 'default' => [
            'importedFile' => __DIR__ . '/../../_data/languages-empty-string.csv',
            'expectedData' => [
                0 => [
                    0 => '30',
                    1 => 'armenia with null',
                    2 => null,
                    3 => 'b',
                ],
                1 => [
                    0 => '31',
                    1 => 'belarus with empty string',
                    2 => null,
                    3 => 'c',
                ],
                2 => [
                    0 => '32',
                    1 => 'malta',
                    2 => 'b',
                    3 => 'b',
                ],
            ],
        ];
        yield 'string' => [
            'importedFile' => __DIR__ . '/../../_data/languages-empty-string.csv',
            'expectedData' => [
                0 => [
                    0 => '30',
                    1 => 'armenia with null',
                    2 => null, // col without enclosure is null by default
                    3 => null,
                ],
                1 => [
                    0 => '31',
                    1 => 'belarus with empty string',
                    2 => '',
                    3 => 'c',
                ],
                2 => [
                    0 => '32',
                    1 => 'malta',
                    2 => null,
                    3 => null,
                ],
            ],
            'treatValuesAsNull' => ['b'],
        ];
    }

    private function createTable(string $tableName): string
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $data = [
            'name' => $tableName,
            'primaryKeysNames' => ['Id'],
            'columns' => [
                [
                    'name' => 'Id',
                    'definition' => [
                        'type' => 'INT',
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
