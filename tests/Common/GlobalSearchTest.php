<?php

declare(strict_types=1);

namespace Common;

use Keboola\StorageApi\Client;
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
        $assertCallback = function ($searchResult) use ($hashedName) {
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
        $assertCallback = function ($searchResult) use ($hashedRowName) {
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

            var_dump($searchResult);
        };
        $this->retryWithCallback($apiCall, $assertCallback);
    }
}
