<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 7/22/13
 * Time: 1:50 PM
 */

namespace Keboola\Test\Backend\CommonPart2;

use Keboola\StorageApi\Metadata;
use Keboola\Test\Common\MetadataTest;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class SnapshottingTest extends StorageApiTestCase
{

    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testTableSnapshotCreate(): void
    {
        $tableId = $this->initTestTable();
        $table = $this->_client->getTable($tableId);

        $description = 'Test snapshot';
        $snapshotId = $this->_client->createTableSnapshot($tableId, $description);
        $this->assertNotEmpty($snapshotId);

        $snapshot = $this->_client->getSnapshot($snapshotId);

        $this->assertEquals($description, $snapshot['description']);
        $this->assertEquals($table['primaryKey'], $snapshot['table']['primaryKey']);
        $this->assertEquals($table['columns'], $snapshot['table']['columns']);
        $this->assertEquals($table['attributes'], $snapshot['table']['attributes']);
        $this->assertArrayHasKey('creatorToken', $snapshot);
        $this->assertNotEmpty($snapshot['dataFileId']);
    }

    public function testTableSnapshotDelete(): void
    {
        $this->skipTestForBackend([
            self::BACKEND_BIGQUERY
        ], 'Bigquery needs to add permission storage.objects.delete https://keboola.atlassian.net/browse/BIG-81');
        $tableId = $this->initTestTable();
        $table = $this->_client->getTable($tableId);

        $description = 'Test snapshot';
        $snapshotId = $this->_client->createTableSnapshot($tableId, $description);
        $this->assertNotEmpty($snapshotId);

        $snapshot = $this->_client->getSnapshot($snapshotId);

        $this->assertEquals($description, $snapshot['description']);
        $this->assertEquals($table['primaryKey'], $snapshot['table']['primaryKey']);
        $this->assertEquals($table['columns'], $snapshot['table']['columns']);
        $this->assertEquals($table['attributes'], $snapshot['table']['attributes']);
        $this->assertArrayHasKey('creatorToken', $snapshot);
        $this->assertNotEmpty($snapshot['dataFileId']);

        $this->_client->deleteSnapshot($snapshotId);
    }

    public function testCreateTableFromSnapshotWithDifferentName(): void
    {
        $sourceTableId = $this->initTestTable();

        $sourceTable = $this->_client->getTable($sourceTableId);
        $snapshotId = $this->_client->createTableSnapshot($sourceTableId);

        $newTableId = $this->_client->createTableFromSnapshot($this->getTestBucketId(), $snapshotId, 'new-table');
        $newTable = $this->_client->getTable($newTableId);

        $this->assertEquals('new-table', $newTable['name']);

        // table metadata validation
        $expectedMetadata = $this->filterIdAndTimestampFromMetadataArray($sourceTable['metadata']);
        $actualMetadata = $this->filterIdAndTimestampFromMetadataArray($newTable['metadata']);

        $this->assertGreaterThan(0, count($expectedMetadata));
        $this->assertSame($expectedMetadata, $actualMetadata);

        // column metadata validation
        $testCase = $this;
        $expectedMetadata = array_map(
            function ($columnMedata) use ($testCase) {
                return $testCase->filterIdAndTimestampFromMetadataArray($columnMedata);
            },
            $sourceTable['columnMetadata']
        );

        $actualMetadata = array_map(
            function ($columnMedata) use ($testCase) {
                return $testCase->filterIdAndTimestampFromMetadataArray($columnMedata);
            },
            $newTable['columnMetadata']
        );

        $this->assertGreaterThan(0, count($expectedMetadata));
        $this->assertSame($expectedMetadata, $actualMetadata);
    }

    /**
     * @return string table id
     */
    private function initTestTable()
    {
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.camel-case-columns.csv'),
            [
                'primaryKey' => 'Id',
            ]
        );

        $this->_client->setTableAttribute($tableId, 'first', 'some value');
        $this->_client->setTableAttribute($tableId, 'second', 'other value');

        $metadata = new Metadata($this->_client);

        $metadata->postTableMetadata(
            $tableId,
            MetadataTest::TEST_PROVIDER,
            [
                [
                    'key' => 'KBC.createdBy.branch.id',
                    'value' => '1234',
                ],
                [
                    'key' => 'KBC.SomeEnity.metadataKey',
                    'value' => 'some value',
                ],
            ]
        );

        $metadata->postColumnMetadata(
            sprintf('%s.%s', $tableId, 'Id'),
            MetadataTest::TEST_PROVIDER,
            [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'NUMERIC',
                ],
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => '',
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'NUMERIC',
                ],
            ]
        );

        return $tableId;
    }

    public function testGetTableSnapshot(): void
    {
        $sourceTableId = $this->initTestTable();

        $this->_client->createTableSnapshot($sourceTableId, 'my snapshot');
        $snapshotId = $this->_client->createTableSnapshot($sourceTableId, 'second');

        $snapshots = $this->_client->listTableSnapshots($sourceTableId, [
            'limit' => 2,
        ]);
        $this->assertIsArray($snapshots);
        $this->assertCount(2, $snapshots);

        $newestSnapshot = reset($snapshots);
        $this->assertEquals($snapshotId, $newestSnapshot['id']);
        $this->assertEquals('second', $newestSnapshot['description']);
    }

    /**
     * https://github.com/keboola/connection/issues/850
     */
    public function testSnapshotPermissions(): void
    {
        $sourceTableId = $this->initTestTable();

        $snapshotId = $this->_client->createTableSnapshot($sourceTableId, 'my snapshot');
        $this->_client->dropBucket(
            $this->getTestBucketId(),
            [
                'force' => true,
            ]
        );

        $newTableId = $this->_client->createTableFromSnapshot($this->getTestBucketId(self::STAGE_OUT), $snapshotId, 'restored');
        $newTable = $this->_client->getTable($newTableId);
        $this->assertEquals('restored', $newTable['name']);
    }

    private function filterIdAndTimestampFromMetadataArray(array $data)
    {
        return array_map(
            function ($metadata) {
                unset($metadata['id']);
                unset($metadata['timestamp']);
                return $metadata;
            },
            $data
        );
    }
}
