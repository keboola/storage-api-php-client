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
     * @param $filterParams
     * @param $expectedTableContent
     * @dataProvider tableDeleteRowsByFiltersData
     */
    public function testTableDeleteRowsByFilter($filterParams, $expectedTableContent): void
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

    public function tableDeleteRowsByFiltersData()
    {
        $yesterday = new \DateTime('-1 day');
        $tomorrow = new \DateTime('+1 day');

        return [
            // 1st test
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => ['PRG'],
                ],
                [
                    [
                        "3",
                        "ondra",
                        "VAN",
                        "male",
                    ],
                    [
                        "4",
                        "miro",
                        "BRA",
                        "male",
                    ],
                    [
                        "5",
                        "hidden",
                        "",
                        "male",
                    ],
                ],
            ],
            // 2nd test
            [
                [
                    'changedSince' => $yesterday->getTimestamp(),
                ],
                [],
            ],
            // 3rd test
            [
                [],
                [],
            ],
            // 4th test
            [
                [
                    'whereOperator' => 'ne',
                    'whereColumn' => 'city',
                    'whereValues' => ['PRG'],
                ],
                [
                    [
                        "1",
                        "martin",
                        "PRG",
                        "male",
                    ],
                    [
                        "2",
                        "klara",
                        "PRG",
                        "female",
                    ],
                ],
            ],
            // 5th test
            [
                [
                    'whereOperator' => 'ne',
                    'whereColumn' => 'city',
                    'whereValues' => ['PRG', 'BRA'],
                ],
                [
                    [
                        "1",
                        "martin",
                        "PRG",
                        "male",
                    ],
                    [
                        "2",
                        "klara",
                        "PRG",
                        "female",
                    ],
                    [
                        "4",
                        "miro",
                        "BRA",
                        "male",
                    ],
                ],
            ],
            // 6th test
            [
                [
                    'changedSince' => $tomorrow->getTimestamp(),
                ],
                [
                    [
                        "1",
                        "martin",
                        "PRG",
                        "male",
                    ],
                    [
                        "2",
                        "klara",
                        "PRG",
                        "female",
                    ],
                    [
                        "3",
                        "ondra",
                        "VAN",
                        "male",
                    ],
                    [
                        "4",
                        "miro",
                        "BRA",
                        "male",
                    ],
                    [
                        "5",
                        "hidden",
                        "",
                        "male",
                    ],
                ],
            ],
        ];
    }
}
