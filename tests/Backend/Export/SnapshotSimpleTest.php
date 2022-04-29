<?php
namespace Keboola\Test\Backend\Export;

use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class SnapshotSimpleTest extends StorageApiTestCase
{
    /**
     * @return void
     */
    public function setUp()
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    /**
     * Copy of SnapshottingTest::testTableSnapshotCreate
     * @see \Keboola\Test\Backend\CommonPart2\SnapshottingTest::testTableSnapshotCreate
     *
     * @return void
     */
    public function testTableSnapshot()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $csvFile = new CsvFile($importFile);
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'users', $csvFile, array(
            'columns' => $csvFile->getHeader(),
        ));
        $table = $this->_client->getTable($tableId);

        $description = 'Test snapshot';
        $snapshotId = $this->_client->createTableSnapshot($tableId, $description);

        $snapshot = $this->_client->getSnapshot($snapshotId);

        $this->assertEquals($description, $snapshot['description']);
        $this->assertEquals($table['primaryKey'], $snapshot['table']['primaryKey']);
        $this->assertEquals($table['columns'], $snapshot['table']['columns']);
        $this->assertEquals($table['attributes'], $snapshot['table']['attributes']);
        $this->assertArrayHasKey('creatorToken', $snapshot);
        $this->assertNotEmpty($snapshot['dataFileId']);
    }
}
