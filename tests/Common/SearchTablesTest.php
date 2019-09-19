<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\Metadata;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class SearchTablesTest extends StorageApiTestCase
{
    const TEST_PROVIDER = "keboola.sapi_client_tests";

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testSearchTablesNoResult()
    {
        $result = $this->_client->searchTables([
            'metadataKey' => 'nonexisting.key',
        ]);
        $this->assertCount(0, $result);
    }

    public function testSearchTables()
    {
        $this->_initTable('table1', [
            [
                "key" => "testkey",
                "value" => "testValue",
            ],
        ]);
        $this->_initTable('table2', [
            [
                "key" => "differentkey",
                "value" => "differentValue",
            ],
        ]);

        $result = $this->_client->searchTables([
            'metadataKey' => 'testkey',
        ]);
        $this->assertCount(1, $result);

        $result = $this->_client->searchTables([
            'metadataValue' => 'testValue',
        ]);
        $this->assertCount(1, $result);

        $result = $this->_client->searchTables([
            'metadataProvider' => self::TEST_PROVIDER,
        ]);
        $this->assertCount(2, $result);

        $result = $this->_client->searchTables([
            'metadataKey' => 'testkey',
            'metadataValue' => 'testValue',
            'metadataProvider' => self::TEST_PROVIDER,
        ]);
        $this->assertCount(1, $result);

        $result = $this->_client->searchTables([]);
        $this->assertCount(2, $result);
    }

    private function _initTable(string $tableName, array $metadata)
    {
        $metadataApi = new Metadata($this->_client);
        $tableId = $this->_client->createTable($this->getTestBucketId(), $tableName, new CsvFile(__DIR__ . '/../_data/languages.csv'));

        $metadataApi->postTableMetadata(
            $tableId,
            self::TEST_PROVIDER,
            $metadata
        );
    }
}
