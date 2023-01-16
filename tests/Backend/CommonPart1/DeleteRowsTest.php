<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\Test\Backend\CommonPart1;

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
     * @param $filterParams
     * @param $expectedTableContent
     */
    public function testTableDeleteRowsByFilter(array $filterParams, array $expectedTableContent): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        $this->_client->deleteTableRows($tableId, $filterParams);
        $tableInfo = $this->_client->getTable($tableId);

        $data = $this->_client->getTableDataPreview($tableId);

        $parsedData = Client::parseCsv($data, false);
        array_shift($parsedData); // remove header

        $this->assertArrayEqualsSorted($expectedTableContent, $parsedData, 0);
    }

    /**
     * @dataProvider tableDeleteRowsByFiltersData
     * @param $filterParams
     * @param $expectedTableContent
     */
    public function testTableDeleteRowsByFilterAsQuery(array $filterParams, array $expectedTableContent): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        $this->_client->deleteTableRowsAsQuery($tableId, $filterParams);
        $tableInfo = $this->_client->getTable($tableId);

        $data = $this->_client->getTableDataPreview($tableId);

        $parsedData = Client::parseCsv($data, false);
        array_shift($parsedData); // remove header

        $this->assertArrayEqualsSorted($expectedTableContent, $parsedData, 0);
    }

    public function testDeleteRowsMissingValuesShouldReturnUserError(): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        try {
            $this->_client->deleteTableRows($tableId, [
                'whereColumn' => 'city',
            ]);
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.validation.invalidFilterValues', $e->getStringCode());
        }
    }

    public function tableDeleteRowsByFiltersData(): array
    {
        $yesterday = new \DateTime('-1 day');
        $tomorrow = new \DateTime('+1 day');

        return [
            'no params' => [
                [],
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
}
