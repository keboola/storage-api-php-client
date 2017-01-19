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

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    /**
     * @param $filterParams
     * @param $expectedTableContent
     * @dataProvider tableDeleteRowsByFiltersData
     */
    public function testTableDeleteRowsByFilter($filterParams, $expectedTableContent)
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));
        $this->_client->markTableColumnAsIndexed($tableId, 'city');

        $this->_client->deleteTableRows($tableId, $filterParams);
        $tableInfo = $this->_client->getTable($tableId);

        $data = $this->_client->exportTable($tableId);

        $parsedData = Client::parseCsv($data, false);
        array_shift($parsedData); // remove header

        $this->assertArrayEqualsSorted($expectedTableContent, $parsedData, 0);
    }

    public function testDeleteRowsMissingValuesShouldReturnUserError()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));
        $this->_client->markTableColumnAsIndexed($tableId, 'city');

        try {
            $this->_client->deleteTableRows($tableId, array(
                'whereColumn' => 'city',
            ));
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.validation.invalidFilterValues', $e->getStringCode());
        }
    }

    public function tableDeleteRowsByFiltersData()
    {
        $yesterday = new \DateTime('-1 day');
        $tomorrow = new \DateTime('+1 day');

        return array(
            // 1st test
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array('PRG')
                ),
                array(
                    array(
                        "3",
                        "ondra",
                        "VAN",
                        "male"
                    ),
                    array(
                        "4",
                        "miro",
                        "BRA",
                        "male",
                    ),
                    array(
                        "5",
                        "hidden",
                        "",
                        "male",
                    ),
                ),
            ),
            // 2nd test
            array(
                array(
                    'changedSince' => $yesterday->getTimestamp(),
                ),
                array(),
            ),
            // 3rd test
            array(
                array(),
                array(),
            ),
            // 4th test
            array(
                array(
                    'whereOperator' => 'ne',
                    'whereColumn' => 'city',
                    'whereValues' => array('PRG')
                ),
                array(
                    array(
                        "1",
                        "martin",
                        "PRG",
                        "male"
                    ),
                    array(
                        "2",
                        "klara",
                        "PRG",
                        "female",
                    ),
                ),
            ),
            // 5th test
            array(
                array(
                    'whereOperator' => 'ne',
                    'whereColumn' => 'city',
                    'whereValues' => array('PRG', 'BRA')
                ),
                array(
                    array(
                        "1",
                        "martin",
                        "PRG",
                        "male"
                    ),
                    array(
                        "2",
                        "klara",
                        "PRG",
                        "female",
                    ),
                    array(
                        "4",
                        "miro",
                        "BRA",
                        "male",
                    ),
                ),
            ),
            // 6th test
            array(
                array(
                    'changedSince' => $tomorrow->getTimestamp(),
                ),
                array(
                    array(
                        "1",
                        "martin",
                        "PRG",
                        "male"
                    ),
                    array(
                        "2",
                        "klara",
                        "PRG",
                        "female",
                    ),
                    array(
                        "3",
                        "ondra",
                        "VAN",
                        "male",
                    ),
                    array(
                        "4",
                        "miro",
                        "BRA",
                        "male",
                    ),
                    array(
                        "5",
                        "hidden",
                        "",
                        "male",
                    ),
                ),
            ),
        );
    }
}
