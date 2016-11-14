<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Redshift;

use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class DedupeTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testDedupe()
    {
        $importFile = __DIR__ . '/../../_data/languages.duplicates.csv';

        // create and import data into source table
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(),
            'languages',
            new CsvFile($importFile),
            array(
                'primaryKey' => 'id'
            )
        );

        $this->assertLinesEqualsSorted(file_get_contents(__DIR__ . '/../../_data/languages.duplicates.deduped.csv'), $this->_client->exportTable($tableId));

        $duplicityResponse = $this->_client->apiGet("storage/tables/{$tableId}/duplicity");
        $this->assertEquals(1, $duplicityResponse['maxDuplicity']);

        // it still should be same
        $this->_client->apiPost("storage/tables/{$tableId}/dedupe");
        $this->assertLinesEqualsSorted(file_get_contents(__DIR__ . '/../../_data/languages.duplicates.deduped.csv'), $this->_client->exportTable($tableId));
    }
}
