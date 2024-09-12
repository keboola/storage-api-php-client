<?php

declare(strict_types=1);

namespace Common;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\BucketUpdateOptions;
use Keboola\StorageApi\Options\Components\Configuration;
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

    /**
     * @group global-search
     */
    public function testConfigurationResponse(): void
    {
        $name = 'main-'.$this->generateDescriptionForTestObject();
        $hashedName = sha1($name);
        $components = new Components($this->client);

        $configuration = new Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName($hashedName)
            ->setDescription('some desc');

        $components->addConfiguration($configuration);

        $apiCall = fn() => $this->client->globalSearch($hashedName);
        $assertCallback = function ($searchResult) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertArrayHasKey('id', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('type', $searchResult['items'][0], 'GlobalSearch');
            $this->assertSame('configuration', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertArrayHasKey('name', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('fullPath', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('project', $searchResult['items'][0]['fullPath'], 'GlobalSearch');
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['project'], 'GlobalSearch');
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['project'], 'GlobalSearch');
            $this->assertArrayHasKey('branch', $searchResult['items'][0]['fullPath'], 'GlobalSearch');
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['branch'], 'GlobalSearch');
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['branch'], 'GlobalSearch');
            $this->assertArrayHasKey('isDefault', $searchResult['items'][0]['fullPath']['branch'], 'GlobalSearch');
            $this->assertArrayHasKey('isNative', $searchResult['items'][0]['fullPath']['branch'], 'GlobalSearch');
            $this->assertArrayHasKey('component', $searchResult['items'][0]['fullPath'], 'GlobalSearch');
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['component'], 'GlobalSearch');
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['component'], 'GlobalSearch');
            $this->assertArrayHasKey('componentId', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('organizationId', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('projectId', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('created', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('byType', $searchResult, 'GlobalSearch');
            $this->assertArrayHasKey('configuration', $searchResult['byType'], 'GlobalSearch');
            $this->assertArrayHasKey('byProject', $searchResult, 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);
    }

    /**
     * @group global-search
     */
    public function testConfigurationRowResponse(): void
    {
        $rowName = 'main-1'.$this->generateDescriptionForTestObject();
        $hashedRowName = sha1($rowName);
        $components = new Components($this->client);

        $configuration = new Configuration();
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
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertArrayHasKey('id', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('type', $searchResult['items'][0], 'GlobalSearch');
            $this->assertSame('configuration-row', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertArrayHasKey('name', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('fullPath', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('project', $searchResult['items'][0]['fullPath'], 'GlobalSearch');
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['project'], 'GlobalSearch');
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['project'], 'GlobalSearch');
            $this->assertArrayHasKey('branch', $searchResult['items'][0]['fullPath'], 'GlobalSearch');
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['branch'], 'GlobalSearch');
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['branch'], 'GlobalSearch');
            $this->assertArrayHasKey('isDefault', $searchResult['items'][0]['fullPath']['branch'], 'GlobalSearch');
            $this->assertArrayHasKey('isNative', $searchResult['items'][0]['fullPath']['branch'], 'GlobalSearch');
            $this->assertArrayHasKey('component', $searchResult['items'][0]['fullPath'], 'GlobalSearch');
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['component'], 'GlobalSearch');
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['component'], 'GlobalSearch');
            $this->assertArrayHasKey('configuration', $searchResult['items'][0]['fullPath'], 'GlobalSearch');
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['configuration'], 'GlobalSearch');
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['configuration'], 'GlobalSearch');
            $this->assertArrayHasKey('componentId', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('organizationId', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('projectId', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('created', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('byType', $searchResult, 'GlobalSearch');
            $this->assertArrayHasKey('configuration-row', $searchResult['byType'], 'GlobalSearch');
            $this->assertArrayHasKey('byProject', $searchResult, 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);
    }

    /**
     * @group global-search
     */
    public function testBucketTableResponse(): void
    {
        $bucketName = 'main'.$this->generateDescriptionForTestObject();
        $hashedBucketName = sha1($bucketName);

        $testBucketId = 'in.c-' . $hashedBucketName;
        $this->dropBucketIfExists($this->_client, $testBucketId, true);
        $this->_client->createBucket($hashedBucketName, 'in', $bucketName);

        $apiCall = fn() => $this->client->globalSearch($hashedBucketName);
        $assertCallback = function ($searchResult) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertArrayHasKey('id', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('type', $searchResult['items'][0], 'GlobalSearch');
            $this->assertSame('bucket', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertArrayHasKey('uri', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('name', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('fullPath', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('project', $searchResult['items'][0]['fullPath'], 'GlobalSearch');
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['project'], 'GlobalSearch');
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['project'], 'GlobalSearch');
            $this->assertArrayHasKey('branch', $searchResult['items'][0]['fullPath'], 'GlobalSearch');
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['branch'], 'GlobalSearch');
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['branch'], 'GlobalSearch');
            $this->assertArrayHasKey('isDefault', $searchResult['items'][0]['fullPath']['branch'], 'GlobalSearch');
            $this->assertArrayHasKey('isNative', $searchResult['items'][0]['fullPath']['branch'], 'GlobalSearch');
            $this->assertArrayHasKey('organizationId', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('projectId', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('created', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('byType', $searchResult, 'GlobalSearch');
            $this->assertArrayHasKey('bucket', $searchResult['byType'], 'GlobalSearch');
            $this->assertArrayHasKey('byProject', $searchResult, 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $tableName = 'table'.$this->generateDescriptionForTestObject();
        $hashedTableName = sha1($tableName);
        $this->_client->createTableAsync($testBucketId, $hashedTableName, new CsvFile(__DIR__ . '/../_data/languages.csv'));
        $apiCall = fn() => $this->client->globalSearch($hashedTableName);
        $assertCallback = function ($searchResult) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertArrayHasKey('id', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('type', $searchResult['items'][0], 'GlobalSearch');
            $this->assertSame('table', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertArrayHasKey('uri', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('name', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('fullPath', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('project', $searchResult['items'][0]['fullPath'], 'GlobalSearch');
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['project'], 'GlobalSearch');
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['project'], 'GlobalSearch');
            $this->assertArrayHasKey('branch', $searchResult['items'][0]['fullPath'], 'GlobalSearch');
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['branch'], 'GlobalSearch');
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['branch'], 'GlobalSearch');
            $this->assertArrayHasKey('isDefault', $searchResult['items'][0]['fullPath']['branch'], 'GlobalSearch');
            $this->assertArrayHasKey('isNative', $searchResult['items'][0]['fullPath']['branch'], 'GlobalSearch');
            $this->assertArrayHasKey('bucket', $searchResult['items'][0]['fullPath'], 'GlobalSearch');
            $this->assertArrayHasKey('id', $searchResult['items'][0]['fullPath']['bucket'], 'GlobalSearch');
            $this->assertArrayHasKey('name', $searchResult['items'][0]['fullPath']['bucket'], 'GlobalSearch');
            $this->assertArrayHasKey('organizationId', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('projectId', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('created', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('byType', $searchResult, 'GlobalSearch');
            $this->assertArrayHasKey('table', $searchResult['byType'], 'GlobalSearch');
            $this->assertArrayHasKey('byProject', $searchResult, 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $newDisplayName = 'DisplayName'.$this->generateDescriptionForTestObject();
        $hashedDisplayName = sha1($newDisplayName);
        $bucketUpdateOptions = new BucketUpdateOptions($testBucketId, $hashedDisplayName, true);
        $this->_client->updateBucket($bucketUpdateOptions);

        $apiCall = fn() => $this->client->globalSearch($hashedDisplayName);
        $assertCallback = function ($searchResult) use ($hashedDisplayName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('bucket', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedDisplayName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $this->_client->dropBucket($testBucketId, ['force' => true]);
        $apiCall = fn() => $this->client->globalSearch($hashedDisplayName);
        $assertCallback = function ($searchResult) {
            $this->assertSame(0, $searchResult['all'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);
    }

    /**
     * @group global-search
     */
    public function testGlobalSearchTogether(): void
    {
        $configurationName = 'main-'.$this->generateDescriptionForTestObject();
        $configurationHashedName = sha1($configurationName);
        $components = new Components($this->client);

        $configuration1 = new Configuration();
        $configuration1
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName($configurationHashedName)
            ->setDescription('description of first configuration');

        $components->addConfiguration($configuration1);

        $rowName = 'main-1'.$this->generateDescriptionForTestObject();
        $hashedRowName = sha1($rowName);
        $components = new Components($this->client);

        $configuration2 = new Configuration();
        $configurationName2 = 'main2-'.$this->generateDescriptionForTestObject();
        $configurationHashedName2 = sha1($configurationName2);
        $configuration2
            ->setComponentId('wr-db')
            ->setConfigurationId('main-2')
            ->setName($configurationHashedName2.' 2 HR')
            ->setDescription('description of second configuration');

        $components->addConfiguration($configuration2);

        $components->addConfigurationRow(
            (new ConfigurationRow($configuration2))
                ->setName($hashedRowName)
                ->setRowId('main-1-row-1'),
        );

        $bucketName1 = 'main'.$this->generateDescriptionForTestObject();
        $hashedBucketName1 = sha1($bucketName1);

        $testBucketId1 = 'in.c-' . $hashedBucketName1;
        $this->dropBucketIfExists($this->_client, $testBucketId1, true);
        $this->_client->createBucket($hashedBucketName1, 'in', $bucketName1);

        $bucketName2 = 'main'.$this->generateDescriptionForTestObject();
        $hashedBucketName2 = sha1($bucketName2);

        $testBucketId2 = 'in.c-' . $hashedBucketName2;
        $this->dropBucketIfExists($this->_client, $testBucketId2, true);
        $this->_client->createBucket($hashedBucketName2, 'in', $bucketName2);

        $tableName1 = 'table'.$this->generateDescriptionForTestObject();
        $hashedTableName1 = sha1($tableName1);
        $this->_client->createTableAsync($testBucketId2, $hashedTableName1, new CsvFile(__DIR__ . '/../_data/languages.csv'));

        $apiCall1 = fn() => $this->client->globalSearch($configurationHashedName);
        $assertCallback1 = function ($searchResult) use ($configurationHashedName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertArrayHasKey('id', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('type', $searchResult['items'][0], 'GlobalSearch');
            $this->assertEquals('configuration', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertArrayHasKey('name', $searchResult['items'][0], 'GlobalSearch');
            $this->assertEquals($searchResult['items'][0]['name'], $configurationHashedName, 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall1, $assertCallback1);

        $apiCall2 = fn() => $this->client->globalSearch($configurationHashedName2);
        $assertCallback2 = function ($searchResult) use ($configurationHashedName2) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertArrayHasKey('id', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('type', $searchResult['items'][0], 'GlobalSearch');
            $this->assertEquals('configuration', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertArrayHasKey('name', $searchResult['items'][0], 'GlobalSearch');
            $this->assertStringStartsWith($configurationHashedName2, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall2, $assertCallback2);

        $apiCall3 = fn() => $this->client->globalSearch($hashedBucketName1);
        $assertCallback3 = function ($searchResult) use ($hashedBucketName1) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertArrayHasKey('id', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('type', $searchResult['items'][0], 'GlobalSearch');
            $this->assertEquals('bucket', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertArrayHasKey('name', $searchResult['items'][0], 'GlobalSearch');
            $this->assertStringStartsWith($hashedBucketName1, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall3, $assertCallback3);

        $apiCall4 = fn() => $this->client->globalSearch($hashedTableName1);
        $assertCallback4 = function ($searchResult) use ($hashedTableName1) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertArrayHasKey('id', $searchResult['items'][0], 'GlobalSearch');
            $this->assertArrayHasKey('type', $searchResult['items'][0], 'GlobalSearch');
            $this->assertEquals('table', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertArrayHasKey('name', $searchResult['items'][0], 'GlobalSearch');
            $this->assertStringStartsWith($hashedTableName1, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall4, $assertCallback4);
    }
}
