<?php
namespace Keboola\Test\Backend\Synapse;

use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class SnapshottingTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testCreateTableFromSnapshotWithDistributionKey(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $importFile = __DIR__ . '/../../_data/languages.csv';

        // create table with distributionKey
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile($importFile),
            [
                'distributionKey' => 'name',
            ],
        );

        $table = $this->_client->getTable($tableId);
        self::assertEquals(['name'], $table['distributionKey']);

        $snapshotId = $this->_client->createTableSnapshot($tableId);

        $newTableId = $this->_client->createTableFromSnapshot($bucketId, $snapshotId, 'languages-restored');
        $newTable = $this->_client->getTable($newTableId);
        self::assertSame($table['distributionKey'], $newTable['distributionKey']);

        // create table with primaryKey
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'languages2',
            new CsvFile($importFile),
            [
                'primaryKey' => 'name',
            ],
        );

        $table = $this->_client->getTable($tableId);
        self::assertEquals(['name'], $table['distributionKey']);

        $snapshotId = $this->_client->createTableSnapshot($tableId);

        $newTableId = $this->_client->createTableFromSnapshot($bucketId, $snapshotId, 'languages2-restored');
        $newTable = $this->_client->getTable($newTableId);
        self::assertSame($table['distributionKey'], $newTable['distributionKey']);
    }
}
