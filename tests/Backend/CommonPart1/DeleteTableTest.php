<?php

namespace Keboola\Test\Backend\CommonPart1;

use Generator;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class DeleteTableTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    /**
     * @dataProvider asyncProvider
     */
    public function testTableDelete(bool $async): void
    {
        $hashedUniqueTableName = sha1('languages-'.$this->generateDescriptionForTestObject());
        $table1Id = $this->_client->createTableAsync($this->getTestBucketId(), $hashedUniqueTableName, new CsvFile(__DIR__ . '/../../_data/languages.csv'));
        $table2Id = $this->_client->createTableAsync($this->getTestBucketId(), 'languages_2', new CsvFile(__DIR__ . '/../../_data/languages.csv'));

        $apiCall = fn() => $this->_client->globalSearch($hashedUniqueTableName);
        $assertCallback = function ($searchResult) use ($hashedUniqueTableName) {
            $this->assertSame(1, $searchResult['all']);
            $this->assertSame('table', $searchResult['items'][0]['type']);
            $this->assertSame($hashedUniqueTableName, $searchResult['items'][0]['name']);
        };
        $this->retryWithCallback($apiCall, $assertCallback);
        $tables = $this->_client->listTables($this->getTestBucketId());

        $this->assertCount(2, $tables);
        $this->_client->dropTable($table1Id, ['async' => $async]);

        $apiCall = fn() => $this->_client->globalSearch($hashedUniqueTableName);
        $assertCallback = function ($searchResult) use ($hashedUniqueTableName) {
            $this->assertSame(0, $searchResult['all']);
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $tables = $this->_client->listTables($this->getTestBucketId());
        $this->assertCount(1, $tables);

        $table = reset($tables);
        $this->assertEquals($table2Id, $table['id']);
    }

    public function asyncProvider(): Generator
    {
        yield 'tableDrop async = false' => [false];
        yield 'tableDrop async = true' => [true];
    }
}
