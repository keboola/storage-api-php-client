<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Mysql;

use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;

class BucketsTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testBucketDropError()
    {
        $inBucketId = $this->getTestBucketId(self::STAGE_IN);
        $outBucketId = $this->getTestBucketId(self::STAGE_OUT);
        $tokenData = $this->_client->verifyToken();

        $tableId = $this->_client->createTable(
            $inBucketId,
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            array(
                'primaryKey' => 'id',
            )
        );

        $table = $this->_client->getTable($tableId);
        $this->assertEquals(array('id'), $table['primaryKey']);
        $this->assertEquals(array('id'), $table['indexedColumns']);

        try {
            $this->_client->dropBucket($inBucketId);
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('buckets.deleteNotEmpty', $e->getStringCode());
        }

        $this->_client->createAliasTable(
            $outBucketId,
            $tableId,
            'languages-alias'
        );

        try {
            $this->_client->dropBucket($outBucketId);
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('buckets.deleteNotEmpty', $e->getStringCode());
        }
    }

    public function testBucketDropAliasError()
    {
        $inBucketId = $this->getTestBucketId(self::STAGE_IN);
        $outBucketId = $this->getTestBucketId(self::STAGE_OUT);
        $tokenData = $this->_client->verifyToken();

        $tables = $this->_client->listTables($inBucketId);
        $this->assertCount(0, $tables);

        $tables = $this->_client->listTables($outBucketId);
        $this->assertCount(0, $tables);

        $tableId = $this->_client->createTable(
            $inBucketId,
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            array(
                'primaryKey' => 'id',
            )
        );

        $table = $this->_client->getTable($tableId);
        $this->assertEquals(array('id'), $table['primaryKey']);
        $this->assertEquals(array('id'), $table['indexedColumns']);

        $tableId = $this->_client->createTable(
            $inBucketId,
            'languages_copy',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            array(
                'primaryKey' => 'id',
            )
        );

        $table = $this->_client->getTable($tableId);
        $this->assertEquals(array('id'), $table['primaryKey']);
        $this->assertEquals(array('id'), $table['indexedColumns']);

        $this->_client->createAliasTable(
            $outBucketId,
            $tableId,
            'languages-alias'
        );

        try {
            $this->_client->dropBucket($inBucketId, array('force' => true));
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.dependentObjects', $e->getStringCode());
        }

        $tables = $this->_client->listTables($inBucketId);
        $this->assertCount(2, $tables);

        $tables = $this->_client->listTables($outBucketId);
        $this->assertCount(1, $tables);
    }

    public function testBucketDrop()
    {
        $inBucketId = $this->getTestBucketId(self::STAGE_IN);
        $outBucketId = $this->getTestBucketId(self::STAGE_OUT);
        $tokenData = $this->_client->verifyToken();

        $tableId = $this->_client->createTable(
            $inBucketId,
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            array(
                'primaryKey' => 'id',
            )
        );

        $table = $this->_client->getTable($tableId);
        $this->assertEquals(array('id'), $table['primaryKey']);
        $this->assertEquals(array('id'), $table['indexedColumns']);

        $tableId = $this->_client->createTable(
            $inBucketId,
            'languages_copy',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            array(
                'primaryKey' => 'id',
            )
        );

        $table = $this->_client->getTable($tableId);
        $this->assertEquals(array('id'), $table['primaryKey']);
        $this->assertEquals(array('id'), $table['indexedColumns']);

        $tableId = $this->_client->createTable(
            $outBucketId,
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            array(
                'primaryKey' => 'id',
            )
        );

        $table = $this->_client->getTable($tableId);
        $this->assertEquals(array('id'), $table['primaryKey']);
        $this->assertEquals(array('id'), $table['indexedColumns']);

        $this->_client->createAliasTable(
            $inBucketId,
            $tableId,
            'languages-alias'
        );

        $tables = $this->_client->listTables($inBucketId);
        $this->assertCount(3, $tables);

        $this->_client->dropBucket($inBucketId, array('force' => true));

        $this->assertFalse($this->_client->bucketExists($inBucketId));

        $tables = $this->_client->listTables($outBucketId);
        $this->assertCount(1, $tables);
    }
}
