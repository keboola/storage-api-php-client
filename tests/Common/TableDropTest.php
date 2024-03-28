<?php

namespace Keboola\Test\Common;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;

class TableDropTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function dropTableProvider(): Generator
    {
        yield 'sync' => [false];
        yield 'async' => [true];
    }

    /**
     * @dataProvider dropTableProvider
     */
    public function testTableDrop(bool $isSync): void
    {
        $importFile = __DIR__ . '/../_data/languages.csv';
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'languages',
            new CsvFile($importFile),
        );
        //test drop bucket not exists
        try {
            $this->_client->dropTable('xxx' . $tableId . 'nonsense', ['async' => $isSync]);
            $this->fail('Bucket not exists exception should be thrown');
        } catch (ClientException $e) {
            $this->assertEquals('storage.buckets.notFound', $e->getStringCode());
        }

        // test drop table not exists
        try {
            $this->_client->dropTable($tableId . 'nonsense', ['async' => $isSync]);
            $this->fail('Table not exists exception should be thrown');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.notFound', $e->getStringCode());
        }

        $this->_client->dropTable($tableId, ['async' => $isSync]);
    }
}
