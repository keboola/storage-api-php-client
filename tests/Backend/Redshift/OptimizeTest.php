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

class OptimizeTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testOptimize(): void
    {
        $importFile = __DIR__ . '/../../_data/pk.simple.csv';

        // create and import data into source table
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'MyLanguages_test',
            new CsvFile($importFile),
            [
                'primaryKey' => 'id',
            ],
        );

        $this->_client->apiPostJson("tables/{$tableId}/optimize");

        $this->assertLinesEqualsSorted(file_get_contents(__DIR__ . '/../../_data/pk.simple.loaded.csv'), $this->_client->getTableDataPreview($tableId));

        // lets test that primary key wasn't lost and icrement imports are ok
        $this->_client->writeTableAsync($tableId, new CsvFile(__DIR__ . '/../../_data/pk.simple.increment.csv'), [
            'incremental' => true,
        ]);
        $this->assertLinesEqualsSorted(file_get_contents(__DIR__ . '/../../_data/pk.simple.increment.loaded.csv'), $this->_client->getTableDataPreview($tableId));

        // test that primary key can be deleted
        $this->_client->removeTablePrimaryKey($tableId);
    }
}
