<?php

namespace Keboola\Test\Backend\Synapse;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;

class CreateTableTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testCreateTableWithDistributionKey()
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
            ]
        );

        $table = $this->_client->getTable($tableId);
        self::assertEquals(['name'], $table['distributionKey']);

        // create table with primaryKey
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'languages2',
            new CsvFile($importFile),
            [
                'primaryKey' => 'name',
            ]
        );

        $table = $this->_client->getTable($tableId);
        self::assertEquals(['name'], $table['distributionKey']);

        try {
            $this->_client->createTableAsync(
                $bucketId,
                'languages',
                new CsvFile($importFile),
                [
                    'distributionKey' => ['name', 'id'],
                ]
            );
            self::fail('distributions keys send as array should throw exception');
        } catch (ClientException $e) {
            self::assertEquals(
            // phpcs:ignore
                'distributionKey must be string. Use comma as separator for multiple distribution keys.',
                $e->getMessage()
            );
            self::assertEquals(
                'storage.validation.distributionKey',
                $e->getStringCode()
            );
        }

        try {
            $this->_client->createTableAsync(
                $bucketId,
                'languages',
                new CsvFile($importFile),
                [
                    'distributionKey' => 'name,id',
                ]
            );
            self::fail('Multiple distributions keys should throw exception');
        } catch (ClientException $e) {
            self::assertEquals(
                'Synapse backend only supports one distributionKey.',
                $e->getMessage()
            );
            self::assertEquals(
                'storage.validation.distributionKey',
                $e->getStringCode()
            );
        }
    }
}
