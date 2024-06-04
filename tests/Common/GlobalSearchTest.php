<?php

declare(strict_types=1);

namespace Common;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\BucketUpdateOptions;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\StorageApiTestCase;

class GlobalSearchTest extends StorageApiTestCase
{
    /**
     * @var ClientProvider
     */
    private $clientProvider;

    /**
     * @var Client
     */
    private $client;

    public function setUp(): void
    {
        parent::setUp();

        $this->cleanupConfigurations($this->_client);

        $this->clientProvider = new ClientProvider($this);
        $this->client = $this->clientProvider->createClientForCurrentTest();
    }

    public function testConfigurationResponse(): void
    {
        $name = 'main-'.$this->generateDescriptionForTestObject();
        $hashedName = sha1($name);
        $components = new \Keboola\StorageApi\Components($this->client);

        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName($hashedName)
            ->setDescription('some desc');

        $components->addConfiguration($configuration);

        $apiCall = fn() => $this->client->globalSearch($hashedName);
        $assertCallback = function ($searchResult) {
            $this->assertSame(1, $searchResult['all']);
            $this->assertArrayHasKey('id', $searchResult['items'][0]);
            $this->assertArrayHasKey('type', $searchResult['items'][0]);
            $this->assertSame('configuration', $searchResult['items'][0]['type']);
            $this->assertArrayHasKey('name', $searchResult['items'][0]);
            $this->assertArrayHasKey('fullPath', $searchResult['items'][0]);
            $this->assertArrayHasKey('project', $searchResult['items'][0]['fullPath']);
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['project']);
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['project']);
            $this->assertArrayHasKey('branch', $searchResult['items'][0]['fullPath']);
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['branch']);
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['branch']);
            $this->assertArrayHasKey('isDefault', $searchResult['items'][0]['fullPath']['branch']);
            $this->assertArrayHasKey('isNative', $searchResult['items'][0]['fullPath']['branch']);
            $this->assertArrayHasKey('component', $searchResult['items'][0]['fullPath']);
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['component']);
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['component']);
            $this->assertArrayHasKey('componentId', $searchResult['items'][0]);
            $this->assertArrayHasKey('organizationId', $searchResult['items'][0]);
            $this->assertArrayHasKey('projectId', $searchResult['items'][0]);
            $this->assertArrayHasKey('created', $searchResult['items'][0]);
            $this->assertArrayHasKey('aggregations', $searchResult);
            $this->assertArrayHasKey('configuration', $searchResult['aggregations']);
        };
        $this->retryWithCallback($apiCall, $assertCallback);
    }

    public function testConfigurationRowResponse(): void
    {
        $rowName = 'main-1'.$this->generateDescriptionForTestObject();
        $hashedRowName = sha1($rowName);
        $components = new \Keboola\StorageApi\Components($this->client);

        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc');

        $components->addConfiguration($configuration);

        $components->addConfigurationRow(
            (new ConfigurationRow($configuration))
                ->setName($hashedRowName)
                ->setRowId('main-1-row-1'),
        );

        $apiCall = fn() => $this->client->globalSearch($hashedRowName);
        $assertCallback = function ($searchResult) {
            $this->assertSame(1, $searchResult['all']);
            $this->assertArrayHasKey('id', $searchResult['items'][0]);
            $this->assertArrayHasKey('type', $searchResult['items'][0]);
            $this->assertSame('configuration-row', $searchResult['items'][0]['type']);
            $this->assertArrayHasKey('name', $searchResult['items'][0]);
            $this->assertArrayHasKey('fullPath', $searchResult['items'][0]);
            $this->assertArrayHasKey('project', $searchResult['items'][0]['fullPath']);
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['project']);
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['project']);
            $this->assertArrayHasKey('branch', $searchResult['items'][0]['fullPath']);
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['branch']);
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['branch']);
            $this->assertArrayHasKey('isDefault', $searchResult['items'][0]['fullPath']['branch']);
            $this->assertArrayHasKey('isNative', $searchResult['items'][0]['fullPath']['branch']);
            $this->assertArrayHasKey('component', $searchResult['items'][0]['fullPath']);
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['component']);
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['component']);
            $this->assertArrayHasKey('configuration', $searchResult['items'][0]['fullPath']);
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['configuration']);
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['configuration']);
            $this->assertArrayHasKey('componentId', $searchResult['items'][0]);
            $this->assertArrayHasKey('organizationId', $searchResult['items'][0]);
            $this->assertArrayHasKey('projectId', $searchResult['items'][0]);
            $this->assertArrayHasKey('created', $searchResult['items'][0]);
            $this->assertArrayHasKey('aggregations', $searchResult);
            $this->assertArrayHasKey('configuration-row', $searchResult['aggregations']);
        };
        $this->retryWithCallback($apiCall, $assertCallback);
    }

    public function testBucketTableResponse(): void
    {
        $bucketName = 'main'.$this->generateDescriptionForTestObject();
        $hashedBucketName = sha1($bucketName);

        $testBucketId = 'in.c-' . $hashedBucketName;
        $this->dropBucketIfExists($this->_client, $testBucketId, true);
        $this->_client->createBucket($hashedBucketName, 'in', $bucketName);

        $apiCall = fn() => $this->client->globalSearch($hashedBucketName);
        $assertCallback = function ($searchResult) {
            $this->assertSame(1, $searchResult['all']);
            $this->assertArrayHasKey('id', $searchResult['items'][0]);
            $this->assertArrayHasKey('type', $searchResult['items'][0]);
            $this->assertSame('bucket', $searchResult['items'][0]['type']);
            $this->assertArrayHasKey('uri', $searchResult['items'][0]);
            $this->assertArrayHasKey('name', $searchResult['items'][0]);
            $this->assertArrayHasKey('fullPath', $searchResult['items'][0]);
            $this->assertArrayHasKey('project', $searchResult['items'][0]['fullPath']);
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['project']);
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['project']);
            $this->assertArrayHasKey('branch', $searchResult['items'][0]['fullPath']);
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['branch']);
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['branch']);
            $this->assertArrayHasKey('isDefault', $searchResult['items'][0]['fullPath']['branch']);
            $this->assertArrayHasKey('isNative', $searchResult['items'][0]['fullPath']['branch']);
            $this->assertArrayHasKey('organizationId', $searchResult['items'][0]);
            $this->assertArrayHasKey('projectId', $searchResult['items'][0]);
            $this->assertArrayHasKey('created', $searchResult['items'][0]);
            $this->assertArrayHasKey('aggregations', $searchResult);
            $this->assertArrayHasKey('bucket', $searchResult['aggregations']);
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $tableName = 'table'.$this->generateDescriptionForTestObject();
        $hashedTableName = sha1($tableName);
        $this->_client->createTableAsync($testBucketId, $hashedTableName, new \Keboola\Csv\CsvFile(__DIR__ . '/../_data/languages.csv'));
        $apiCall = fn() => $this->client->globalSearch($hashedTableName);
        $assertCallback = function ($searchResult) {
            $this->assertSame(1, $searchResult['all']);
            $this->assertArrayHasKey('id', $searchResult['items'][0]);
            $this->assertArrayHasKey('type', $searchResult['items'][0]);
            $this->assertSame('table', $searchResult['items'][0]['type']);
            $this->assertArrayHasKey('uri', $searchResult['items'][0]);
            $this->assertArrayHasKey('name', $searchResult['items'][0]);
            $this->assertArrayHasKey('fullPath', $searchResult['items'][0]);
            $this->assertArrayHasKey('project', $searchResult['items'][0]['fullPath']);
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['project']);
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['project']);
            $this->assertArrayHasKey('branch', $searchResult['items'][0]['fullPath']);
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['branch']);
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['branch']);
            $this->assertArrayHasKey('isDefault', $searchResult['items'][0]['fullPath']['branch']);
            $this->assertArrayHasKey('isNative', $searchResult['items'][0]['fullPath']['branch']);
            $this->assertArrayHasKey('bucket', $searchResult['items'][0]['fullPath']);
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['bucket']);
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['bucket']);
            $this->assertArrayHasKey('organizationId', $searchResult['items'][0]);
            $this->assertArrayHasKey('projectId', $searchResult['items'][0]);
            $this->assertArrayHasKey('created', $searchResult['items'][0]);
            $this->assertArrayHasKey('aggregations', $searchResult);
            $this->assertArrayHasKey('table', $searchResult['aggregations']);
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $newDisplayName = 'DisplayName'.$this->generateDescriptionForTestObject();
        $hashedDisplayName = sha1($newDisplayName);
        $bucketUpdateOptions = new BucketUpdateOptions($testBucketId, $hashedDisplayName, true);
        $this->_client->updateBucket($bucketUpdateOptions);

        $apiCall = fn() => $this->client->globalSearch($hashedDisplayName);
        $assertCallback = function ($searchResult) use ($hashedDisplayName) {
            $this->assertSame(1, $searchResult['all']);
            $this->assertSame('bucket', $searchResult['items'][0]['type']);
            $this->assertSame($hashedDisplayName, $searchResult['items'][0]['name']);
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $this->_client->dropBucket($testBucketId, ['force' => true]);
        $apiCall = fn() => $this->client->globalSearch($hashedDisplayName);
        $assertCallback = function ($searchResult) {
            $this->assertSame(0, $searchResult['all']);
        };
        $this->retryWithCallback($apiCall, $assertCallback);
    }
}
