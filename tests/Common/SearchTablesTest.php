<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\SearchTablesOptions;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class SearchTablesTest extends StorageApiTestCase
{
    const TEST_PROVIDER = "keboola.sapi_client_tests";

    public function setUp()
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testSearchTablesNoResult()
    {
        $result = $this->_client->searchTables(new SearchTablesOptions('nonexisting.key', null, null));
        $this->assertCount(0, $result);
    }

    public function testSearchTables()
    {
        $testKey = sha1($this->generateDescriptionForTestObject());
        $metadataKey = "testkey" . $testKey;
        $metadataValue = "testValue" . $testKey;
        $provider = self::TEST_PROVIDER . $testKey;
        $this->_initTable('tableX', [
            [
                "key" => $metadataKey,
                "value" => $metadataValue,
            ],
        ], self::STAGE_IN, $provider);
        $this->_initTable('tableX', [], self::STAGE_OUT, $provider); // table in different bucket
        $this->_initTable('tableY', [
            [
                "key" => "differentkey",
                "value" => "differentValue",
            ],
        ], self::STAGE_IN, $provider);
        $this->_initTable('table-nometa', [], self::STAGE_IN, $provider);

        $result = $this->_client->searchTables(
            new SearchTablesOptions($metadataKey, null, null)
        );
        $this->assertCount(1, $result);

        $firstResult = reset($result);
        $this->assertArrayHasKey('displayName', $firstResult['bucket']);
        $this->assertArrayHasKey('displayName', $firstResult);

        $result = $this->_client->searchTables(
            new SearchTablesOptions(null, $metadataValue, null)
        );
        $this->assertCount(1, $result);

        $result = $this->_client->searchTables(
            new SearchTablesOptions(null, null, $provider)
        );
        $this->assertCount(2, $result);

        $result = $this->_client->searchTables(
            new SearchTablesOptions($metadataKey, $metadataValue, $provider)
        );
        $this->assertCount(1, $result);
    }

    public function testSearchTablesEmptyRequest()
    {
        try {
            $this->_client->searchTables(new SearchTablesOptions);
        } catch (ClientException $clientException) {
            $this->assertSame('Invalid request', $clientException->getMessage());
            $this->assertSame([
                [
                    'key' => 'metadataKey',
                    'message' => 'At least on of search parameters metadataKey|metadataValue|metadataProvider must be provided.',
                ],
            ], $clientException->getContextParams()['errors']);
            $this->assertEquals('validationError', $clientException->getContextParams()['code']);
        }
    }

    private function _initTable(
        $tableName,
        array $metadata,
        $stage,
        $metadataProvider
    ) {
        $metadataApi = new Metadata($this->_client);
        $tableId = $this->_client->createTable($this->getTestBucketId($stage), $tableName, new CsvFile(__DIR__ . '/../_data/languages.csv'));

        if (!empty($metadata)) {
            $metadataApi->postTableMetadata(
                $tableId,
                $metadataProvider,
                $metadata
            );
        }
    }
}
