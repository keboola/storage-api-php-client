<?php



namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;

class FulltextSearchTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testFindDataByFulltext(): void
    {
        $tableId = $this->prepareTable();

        $dataPreview = $this->_client->getTableDataPreview($tableId, ['fulltextSearch' => 'containsE']);
        $dataPreviewCsv = Client::parseCsv($dataPreview);

        $this->assertCount(2, $dataPreviewCsv);
        $this->assertSame('DcontainsDD', $dataPreviewCsv[0]['column_2']);
    }

    public function testFindNonExistingDataByFulltext(): void
    {
        $tableId = $this->prepareTable();

        $dataPreview = $this->_client->getTableDataPreview($tableId, ['fulltextSearch' => 'contains-non-existing']);
        $dataPreviewCsv = Client::parseCsv($dataPreview);

        $this->assertCount(0, $dataPreviewCsv);
    }

    public function testFulltextAndWhereFiltersAtTheSameTimeShouldFail(): void
    {
        $tableId = $this->prepareTable();

        $params = [
            'fulltextSearch' => 'ContainsA',
            'whereFilters' => [
                [
                    'column' => 'column_1',
                    'operator' => 'eq',
                    'values' => ['AcontaionsAA'],
                ],
            ],
        ];
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Cannot use fulltextSearch and whereFilters at the same time');
        $this->_client->getTableDataPreview($tableId, $params);
    }

    private function prepareTable()
    {
        $csvFile = $this->createTempCsv();
        $csvFile->writeRow(['column_1', 'column_2', 'column_3']);
        $csvFile->writeRow(['AcontainsAA', 'BcontainsBB', 'CcontainsCC']);
        $csvFile->writeRow(['AcontainsAA', 'DcontainsDD', 'EcontainsEE']);
        $csvFile->writeRow(['DcontainsDD', 'EcontainsEE', 'FcontainsFF']);
        return $this->_client->createTableAsync($this->getTestBucketId(), 'fulltext', $csvFile);
    }
}
