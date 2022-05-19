<?php

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class LegacyIndexedColumnsTest extends StorageApiTestCase
{

    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests([self::STAGE_SYS, self::STAGE_IN, self::STAGE_OUT]);
    }


    public function testIndexApiCallsDoesNotThrowError(): void
    {
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile($importFile));


        $indexedColumnName = 'id';
        $this->_client->apiPost("tables/$tableId/indexed-columns", [
            'name' => $indexedColumnName,
        ]);
        $this->_client->apiDelete("tables/$tableId/indexed-columns/$indexedColumnName");

        $table = $this->_client->getTable($tableId);
        $this->assertArrayHasKey('indexedColumns', $table, 'indexedColumns should still be present in response');
    }
}
