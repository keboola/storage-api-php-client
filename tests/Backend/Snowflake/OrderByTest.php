<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\Test\StorageApiTestCase;

class OrderByTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testSimpleSort(): void
    {
        $tableId = $this->prepareTable();

        $order = [
            'column' => 'column_string',
        ];
        $dataPreview = $this->_client->getTableDataPreview($tableId, ['orderBy' => [$order]]);

        $this->assertSame('aa', Client::parseCsv($dataPreview)[0]['column_string']);
    }

    public function testSortWithDataType(): void
    {
        $tableId = $this->prepareTable();

        $order = [
            'column' => 'column_double',
            'dataType' => 'DOUBLE',
        ];
        $dataPreview = $this->_client->getTableDataPreview($tableId, ['orderBy' => [$order]]);

        $this->assertSame('1.1234', Client::parseCsv($dataPreview)[0]['column_double']);
    }

    public function testComplexSort(): void
    {
        $tableId = $this->prepareTable();

        $order = [
            [
                'column' => 'column_string',
                'order' => 'DESC',
            ],
            [
                'column' => 'column_string_number',
                'order' => 'ASC',
                'dataType' => 'INTEGER'
            ]
        ];

        $dataPreview = $this->_client->getTableDataPreview($tableId, ['orderBy' => $order]);
        $this->assertSame('5', Client::parseCsv($dataPreview)[0]['column_string_number']);
    }

    private function prepareTable(): string
    {
        $csvFile = new CsvFile(tempnam(sys_get_temp_dir(), 'keboola'));
        $csvFile->writeRow(['column_string', 'column_string_number', 'column_double']);
        $csvFile->writeRow(['ab', '1', '003.123']);
        $csvFile->writeRow(['bc', '4', '3.123']);
        $csvFile->writeRow(['aa', '4444', '0004.123']);
        $csvFile->writeRow(['zx', '5', '4']);
        $csvFile->writeRow(['zx', '555111', '1.1234']);
        return $this->_client->createTable($this->getTestBucketId(), 'conditions', $csvFile);
    }
}
