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
        $table1Id = $this->_client->createTableAsync($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../../_data/languages.csv'));
        $table2Id = $this->_client->createTableAsync($this->getTestBucketId(), 'languages_2', new CsvFile(__DIR__ . '/../../_data/languages.csv'));
        $tables = $this->_client->listTables($this->getTestBucketId());

        $this->assertCount(2, $tables);
        $this->_client->dropTable($table1Id, ['async' => $async]);

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
