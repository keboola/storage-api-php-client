<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Mysql;

use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;

class ExportParamsTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    /**
     * @param $exportOptions
     * @param $expectedResult
     * @dataProvider tableExportFiltersData
     */
    public function testTableExportAsyncMysql($exportOptions, $expectedResult)
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'users', new CsvFile($importFile));
        $this->_client->markTableColumnAsIndexed($tableId, 'city');

        $results = $this->_client->exportTableAsync($tableId, $exportOptions);

        $exportedFile = $this->_client->getFile($results['file']['id']);
        $parsedData = Client::parseCsv(file_get_contents($exportedFile['url']), false);
        array_shift($parsedData); // remove header

        $this->assertArrayEqualsSorted($expectedResult, $parsedData, 0);
    }

    public function testTableExportAsyncColumnsParam()
    {
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile($importFile));

        $results = $this->_client->exportTableAsync($tableId, array(
            'columns' => array('id'),
        ));
        $file = $this->_client->getFile($results['file']['id']);
        $parsed = Client::parseCsv(file_get_contents($file['url']), false);
        $firstRow = reset($parsed);

        $this->assertCount(1, $firstRow);
        $this->assertArrayHasKey(0, $firstRow);
        $this->assertEquals("id", $firstRow[0]);
    }

    public function testTableExportAsyncGzip()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'users', new CsvFile($importFile));
        $this->_client->markTableColumnAsIndexed($tableId, 'city');

        $results = $this->_client->exportTableAsync($tableId, array(
            'gzip' => true,
        ));

        $exportedFile = $this->_client->getFile($results['file']['id']);
        $parsedData = Client::parseCsv(gzdecode(file_get_contents($exportedFile['url'])), false);
        array_shift($parsedData); // remove header

        $expected = Client::parseCsv(file_get_contents($importFile), false);
        array_shift($expected);

        $this->assertArrayEqualsSorted($expected, $parsedData, 0);
    }
}
