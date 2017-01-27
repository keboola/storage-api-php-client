<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 7/22/13
 * Time: 1:50 PM
 */

namespace Keboola\Test\Backend\CommonPart2;

use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class SnapshottingTest extends StorageApiTestCase
{

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testTableSnapshotCreate()
    {
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            array(
                'primaryKey' => 'id',
            )
        );

        $this->_client->setTableAttribute($tableId, 'first', 'some value');
        $this->_client->setTableAttribute($tableId, 'second', 'other value');
        $table = $this->_client->getTable($tableId);

        $description = 'Test snapshot';
        $snapshotId = $this->_client->createTableSnapshot($tableId, $description);
        $this->assertNotEmpty($snapshotId);

        $snapshot = $this->_client->getSnapshot($snapshotId);

        $this->assertEquals($description, $snapshot['description']);
        $this->assertEquals($table['primaryKey'], $snapshot['table']['primaryKey']);
        $this->assertEquals($table['columns'], $snapshot['table']['columns']);
        $this->assertEquals($table['indexedColumns'], $snapshot['table']['indexedColumns']);
        $this->assertEquals($table['attributes'], $snapshot['table']['attributes']);
        $this->assertArrayHasKey('creatorToken', $snapshot);
        $this->assertNotEmpty($snapshot['dataFileId']);
    }

    public function testTableSnapshotDelete()
    {
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            array(
                'primaryKey' => 'id',
            )
        );

        $this->_client->setTableAttribute($tableId, 'first', 'some value');
        $this->_client->setTableAttribute($tableId, 'second', 'other value');
        $table = $this->_client->getTable($tableId);

        $description = 'Test snapshot';
        $snapshotId = $this->_client->createTableSnapshot($tableId, $description);
        $this->assertNotEmpty($snapshotId);

        $snapshot = $this->_client->getSnapshot($snapshotId);

        $this->assertEquals($description, $snapshot['description']);
        $this->assertEquals($table['primaryKey'], $snapshot['table']['primaryKey']);
        $this->assertEquals($table['columns'], $snapshot['table']['columns']);
        $this->assertEquals($table['indexedColumns'], $snapshot['table']['indexedColumns']);
        $this->assertEquals($table['attributes'], $snapshot['table']['attributes']);
        $this->assertArrayHasKey('creatorToken', $snapshot);
        $this->assertNotEmpty($snapshot['dataFileId']);

        $this->_client->deleteSnapshot($snapshotId);
    }

    public function testCreateTableFromSnapshotWithDifferentName()
    {
        $sourceTableId = $this->_client->createTable(
            $this->getTestBucketId(),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.camel-case-columns.csv'),
            array(
                'primaryKey' => 'Id',
            )
        );
        $sourceTable = $this->_client->getTable($sourceTableId);
        $snapshotId = $this->_client->createTableSnapshot($sourceTableId);

        $newTableId = $this->_client->createTableFromSnapshot($this->getTestBucketId(), $snapshotId, 'new-table');
        $newTable = $this->_client->getTable($newTableId);

        $this->assertEquals('new-table', $newTable['name']);
    }

    public function testGetTableSnapshot()
    {
        $sourceTableId = $this->_client->createTable(
            $this->getTestBucketId(),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            array(
                'primaryKey' => 'id',
            )
        );

        $this->_client->createTableSnapshot($sourceTableId, 'my snapshot');
        $snapshotId = $this->_client->createTableSnapshot($sourceTableId, 'second');

        $snapshots = $this->_client->listTableSnapshots($sourceTableId, array(
            'limit' => 2,
        ));
        $this->assertInternalType('array', $snapshots);
        $this->assertCount(2, $snapshots);

        $newestSnapshot = reset($snapshots);
        $this->assertEquals($snapshotId, $newestSnapshot['id']);
        $this->assertEquals('second', $newestSnapshot['description']);
    }
}
