<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Components\SearchComponentConfigurationsOptions;
use Keboola\StorageApi\Options\SearchTablesOptions;
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

    public function testSearchThrowsErrorWhenIsCalledWithBranch()
    {
        $defaultBranchId = $this->getDefaultBranchId($this);
        $branchAwareClient = $this->getBranchAwareDefaultClient($defaultBranchId);
        try {
            $branchAwareClient->searchTables((new SearchTablesOptions()));
            $this->fail('should fail, not implemented with branch');
        } catch (ClientException $e) {
            $this->assertContains('Not implemented', $e->getMessage());
            $this->assertSame(501, $e->getCode());
        }
    }

    public function testSearchTablesNoResult()
    {
        $result = $this->_client->searchTables(new SearchTablesOptions('nonexisting.key', null, null));
        $this->assertCount(0, $result);
    }

    public function testSearchTables()
    {
        $this->_initTable('tableX', [
            [
                "key" => "testkey",
                "value" => "testValue",
            ],
        ]);
        $this->_initTable('tableX', [], self::STAGE_OUT); // table in different bucket
        $this->_initTable('tableY', [
            [
                "key" => "differentkey",
                "value" => "differentValue",
            ],
        ]);
        $this->_initTable('table-nometa', []);

        $result = $this->_client->searchTables(
            new SearchTablesOptions('testkey', null, null)
        );
        $this->assertCount(1, $result);

        $firstResult = reset($result);
        $this->assertArrayHasKey('displayName', $firstResult['bucket']);
        $this->assertArrayHasKey('displayName', $firstResult);

        $result = $this->_client->searchTables(
            new SearchTablesOptions(null, 'testValue', null)
        );
        $this->assertCount(1, $result);

        $result = $this->_client->searchTables(
            new SearchTablesOptions(null, null, self::TEST_PROVIDER)
        );
        $this->assertCount(2, $result);

        $result = $this->_client->searchTables(
            new SearchTablesOptions('testkey', 'testValue', self::TEST_PROVIDER)
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
        $stage = self::STAGE_IN
    ) {
        $metadataApi = new Metadata($this->_client);
        $tableId = $this->_client->createTable($this->getTestBucketId($stage), $tableName, new CsvFile(__DIR__ . '/../_data/languages.csv'));

        if (!empty($metadata)) {
            $metadataApi->postTableMetadata(
                $tableId,
                self::TEST_PROVIDER,
                $metadata
            );
        }
    }
}
