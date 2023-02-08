<?php



namespace Keboola\Test\Backend\Redshift;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;

class FulltextSearchTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testForbiddenWhereOperators(): void
    {
        $csvFile = $this->createTempCsv();
        $csvFile->writeRow(['test']);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'conditions', $csvFile);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Fulltext search is not supported for Redshift backend.');
        $this->_client->getTableDataPreview($tableId, ['fulltextSearch' => 'not-supported']);
    }
}
