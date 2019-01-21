<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\CommonPart2;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\Test\StorageApiTestCase;

class DataPreviewFilterTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testDataPreviewWithWhereConditions()
    {
        $csvFile = new CsvFile(tempnam(sys_get_temp_dir(), 'keboola'));
        $csvFile->writeRow(["column_1", "column_2", "column_3"]);
        $csvFile->writeRow(["first",1,2]);
        $csvFile->writeRow(["second",4,5]);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'conditions', $csvFile);

        $where = [
            [
                "column" => "column_1",
                "whereValues" => "eq",
                "val" => "first",
            ]
        ];
        $preview = $this->_client->getTableDataPreview($tableId, ['whereFilters' => $where]);

        $this->assertCount(1, Client::parseCsv($preview));
    }
}
