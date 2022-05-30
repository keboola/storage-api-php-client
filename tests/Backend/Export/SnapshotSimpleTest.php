<?php
namespace Keboola\Test\Backend\Export;

use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class SnapshotSimpleTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    /**
     * Copy of SnapshottingTest::testTableSnapshotCreate
     * @see \Keboola\Test\Backend\CommonPart2\SnapshottingTest::testTableSnapshotCreate
     */
    public function testTableSnapshot(): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $csvFile = new CsvFile($importFile);
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'users', $csvFile, [
            'columns' => $csvFile->getHeader(),
        ]);
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

    /**
     * Copy of SnapshottingTest::testCreateTableFromSnapshotWithDifferentName
     * @see \Keboola\Test\Backend\CommonPart2\SnapshottingTest::testCreateTableFromSnapshotWithDifferentName
     */
    public function testCreateTableFromSnapshot(): void
    {
        // create snapshot
        $importFile = __DIR__ . '/../../_data/users.csv';
        $csvFile = new CsvFile($importFile);
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'users', $csvFile, [
            'columns' => $csvFile->getHeader(),
        ]);
        $sourceTable = $this->_client->getTable($tableId);

        $description = 'Test snapshot';
        $snapshotId = $this->_client->createTableSnapshot($tableId, $description);

        // create table from snapshot
        $newTableId = $this->_client->createTableFromSnapshot($this->getTestBucketId(), $snapshotId, 'new-users');
        $newTable = $this->_client->getTable($newTableId);

        $this->assertEquals('new-users', $newTable['name']);

        $this->assertSame($sourceTable['rowsCount'], $newTable['rowsCount']);
        $this->assertSame($sourceTable['columns'], $newTable['columns']);
    }
}
