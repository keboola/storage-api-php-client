<?php



namespace Keboola\Test\Backend\Redshift;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;

class OrderByTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testForbiddenWhereOperators()
    {
        $csvFile = new CsvFile(tempnam(sys_get_temp_dir(), 'keboola'));
        $csvFile->writeRow(['test']);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'conditions', $csvFile);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Order statements are not supported for Redshift backend.');
        $this->_client->getTableDataPreview($tableId, ['orderBy' => [['column'=>'test']]]);
    }
}
