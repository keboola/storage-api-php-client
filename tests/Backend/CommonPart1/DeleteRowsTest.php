<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;

class DeleteRowsTest extends StorageApiTestCase
{

    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    /**
     * @dataProvider tableDeleteRowsByFiltersData
     */
    public function testTableDeleteRowsByFilter(array $filterParams, array $expectedTableContent): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));
        $tableInfo = $this->_client->getTable($tableId);
        if ($this->isBigqueryWithNewDeleteRows($tableInfo['bucket']['backend'], $filterParams)) {
            $this->markTestSkipped('BQ does not new params of valuesBy* yet');
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
        if ($this->isBigqueryWithNewDeleteRows($tableInfo['bucket']['backend'], $filterParams)) {
            $this->markTestSkipped('BQ does not new params of valuesBy* yet');
        }
        $this->_client->deleteTableRowsAsQuery($tableId, $filterParams);

        $data = $this->_client->getTableDataPreview($tableId);

        $parsedData = Client::parseCsv($data, false);
        array_shift($parsedData); // remove header

        $this->assertArrayEqualsSorted($expectedTableContent, $parsedData, 0);
    }

    // because BQ does not support valuesByTableInStorage / valuesByTableInWorkspace yet/. Tmp fix
    private function isBigqueryWithNewDeleteRows(string $backendName, array $filterParams): bool
    {
        return $backendName === 'bigquery' &&
            array_key_exists('whereFilters', $filterParams) &&
            count($filterParams) > 0 &&
            (array_key_exists('valuesByTableInStorage', $filterParams['whereFilters'][0]) || array_key_exists('valuesByTableInWorkspace', $filterParams['whereFilters'][0])
            );
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
            $this->assertSame('Cannot filter by column "notExistingColumn", column does not exist', $e->getMessage());
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
            'where filter: valuesByTableInWorkspace' => [
                [
                    'whereFilters' => [
                        [
                            'column' => 'city',
                            'valuesByTableInWorkspace' => [
                                    'workspaceId' => 123,
                                    'table' => 'table',
                                    'column' => 'city',
                            ],
                        ],
                    ],
                ],
                // no rows should be deleted because valuesByTableInStorage doesn't do anything yet
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
            'where filter: valuesByTableInStorage' => [
                [
                    'whereFilters' => [
                        [
                            'column' => 'city',
                            'valuesByTableInStorage' => [
                                    'tableId' => 'table',
                                    'column' => 'city',
                            ],
                        ],
                    ],
                ],
                // no rows should be deleted because valuesByTableInStorage doesn't do anything yet
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
        ];
    }
}
