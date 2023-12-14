<?php
namespace Keboola\Test\Common;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\StorageApiTestCase;

class ConfigurationRowTest extends StorageApiTestCase
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

        $components = new \Keboola\StorageApi\Components($this->_client);
        foreach ($components->listComponents() as $component) {
            foreach ($component['configurations'] as $configuration) {
                $components->deleteConfiguration($component['id'], $configuration['id']);
            }
        }

        // erase all deleted configurations
        foreach ($components->listComponents((new ListComponentsOptions())->setIsDeleted(true)) as $component) {
            foreach ($component['configurations'] as $configuration) {
                $components->deleteConfiguration($component['id'], $configuration['id']);
            }
        }

        $this->clientProvider = new ClientProvider($this);
        $this->client = $this->clientProvider->createClientForCurrentTest();
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testConfigurationCopyCreateWithSameRowId(): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);

        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main 1')
            ->setDescription('description');
        $components->addConfiguration($config);

        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $rowConfig->setRowId('main-1-1');
        $rowConfig->setState(['key' => 'main-1-1']);
        $components->addConfigurationRow($rowConfig);

        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $rowConfig->setRowId('main-1-2');
        $rowConfig->setState(['key' => 'main-1-2']);
        $components->addConfigurationRow($rowConfig);

        $config2 = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-2')
            ->setName('Main 2')
            ->setDescription('description');
        $components->addConfiguration($config2);

        $rowConfig2 = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config2);
        $rowConfig2->setRowId('main-1-1'); // same rowId to test create correct copy
        $rowConfig2->setState(['key' => 'main-2-1']);
        $components->addConfigurationRow($rowConfig2);

        $copiedConfig = $components->createConfigurationFromVersion(
            'wr-db',
            $config2->getConfigurationId(),
            2,
            'copy-main',
        );
        $response = $components->getConfiguration('wr-db', $copiedConfig['id']);

        $this->assertSame(['key' => 'main-2-1'], $response['rows'][0]['state']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testConfigurationRowReturnsSingleRow(): void
    {
        $components = new Components($this->client);
        $configuration = new Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $configurationRow1 = new ConfigurationRow($configuration);
        $configurationRow1->setRowId('main-1-1');
        $components->addConfigurationRow($configurationRow1);

        $configurationRow2 = new ConfigurationRow($configuration);
        $configurationRow2->setRowId('main-1-2');
        $components->addConfigurationRow($configurationRow2);

        $row = $components->getConfigurationRow(
            'wr-db',
            'main-1',
            'main-1-2',
        );

        $this->assertEquals('main-1-2', $row['id']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testConfigurationRowWithEmptyStringIdWillGenerateId(): void
    {
        $components = new Components($this->client);
        $configuration = new Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $configurationRow1 = new ConfigurationRow($configuration);
        $configurationRow1->setRowId('');

        $row = $components->addConfigurationRow($configurationRow1);

        $this->assertNotSame('', $row['id']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testConfigurationRowThrowsNotFoundException(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Row invalidRowID not found');

        $components = new Components($this->client);
        $configuration = new Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $components->getConfigurationRow(
            'wr-db',
            'main-1',
            'invalidRowID',
        );
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testConfigurationRowJsonDataTypes(): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        // to check if params is object we have to convert received json to objects instead of assoc array
        // so we have to use raw Http Client
        $guzzleClient = $this->clientProvider->createGuzzleClientForCurrentTest([
            'base_uri' => $this->client->getApiUrl(),
        ], true);

        $config = (object) [
            'test' => 'neco',
            'array' => [],
            'object' => (object) [],
        ];

        $state = (object) [
            'test' => 'state',
            'array' => [],
            'object' => (object) [
                'subobject' => (object) [],
            ],
        ];

        $response = $guzzleClient->post('/v2/storage/branch/default/components/wr-db/configs/main-1/rows', [
            'json' => [
                'name' => 'test',
                'configuration' => json_encode($config),
                'state' => json_encode($state),
            ],
            'headers' => [
                'X-StorageApi-Token' => $this->client->getTokenString(),
            ],
        ]);
        $response = json_decode((string) $response->getBody());
        $this->assertEquals($config, $response->configuration);
        $this->assertEquals($state, $response->state);

        $response = $guzzleClient->get('/v2/storage/branch/default/components/wr-db/configs/main-1/rows/' . $response->id, [
            'headers' => [
                'X-StorageApi-Token' => $this->client->getTokenString(),
            ],
        ]);
        $response = json_decode((string) $response->getBody());
        $this->assertEquals($config, $response->configuration);
        $this->assertEquals($state, $response->state);

        // update
        $config = (object) [
            'test' => 'neco',
            'array' => ['2'],
            'anotherArr' => [],
            'object' => (object) [],
        ];
        $state = (object) [
            'test2' => 'state',
            'array2' => [],
            'object2' => (object) [
                'subobject2' => (object) [],
            ],
        ];

        $guzzleClient->put('/v2/storage/branch/default/components/wr-db/configs/main-1/rows/' . $response->id . '/state', [
            'json' => [
                'state' => json_encode($state),
            ],
            'headers' => [
                'X-StorageApi-Token' => $this->client->getTokenString(),
            ],
        ]);

        $response = $guzzleClient->put('/v2/storage/branch/default/components/wr-db/configs/main-1/rows/' . $response->id, [
            'json' => [
                'configuration' => json_encode($config),
            ],
            'headers' => [
                'X-StorageApi-Token' => $this->client->getTokenString(),
            ],
        ]);
        $response = json_decode((string) $response->getBody());
        $this->assertEquals($config, $response->configuration);
        $this->assertEquals($state, $response->state);

        $response = $guzzleClient->get('/v2/storage/branch/default/components/wr-db/configs/main-1/rows/' . $response->id, [
            'headers' => [
                'X-StorageApi-Token' => $this->client->getTokenString(),
            ],
        ]);
        $response = json_decode((string) $response->getBody());
        $this->assertEquals($config, $response->configuration);
        $this->assertEquals($state, $response->state);
        $this->assertFalse($response->isDisabled);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testConfigurationRowIsDisabledBooleanValue(): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $guzzleClient = $this->clientProvider->createGuzzleClientForCurrentTest([
            'base_uri' => $this->client->getApiUrl(),
        ], true);

        $config = (object) [
            'test' => 'neco',
            'array' => [],
            'object' => (object) [],
        ];

        $state = (object) [
            'test' => 'state',
            'array' => [],
            'object' => (object) [
                'subobject' => (object) [],
            ],
        ];

        $response = $guzzleClient->post('/v2/storage/branch/default/components/wr-db/configs/main-1/rows', [
            'json' => [
                'configuration' => json_encode($config),
                'state' => json_encode($state),
            ],
            'headers' => [
                'X-StorageApi-Token' => $this->client->getTokenString(),
            ],
        ]);
        $response = json_decode((string) $response->getBody());

        $responsePut = $guzzleClient->put('/v2/storage/branch/default/components/wr-db/configs/main-1/rows/' . $response->id, [
            'json' => [
                'isDisabled' => 'true',
                'changeDescription' => 'Row ABCD disabled',
            ],
            'headers' => [
                'X-StorageApi-Token' => $this->client->getTokenString(),
            ],
        ]);

        $result = json_decode((string) $responsePut->getBody());
        $this->assertTrue($result->isDisabled);
        $this->assertEquals('Row ABCD disabled', $result->changeDescription);
    }

    /**
     * @dataProvider isDisabledProvider
     * @param string $clientType
     * @param mixed $isDisabled
     * @param bool $expectedIsDisabled
     */
    public function testCreateConfigurationRowIsDisabled($clientType, $isDisabled, $expectedIsDisabled): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $client = $this->clientProvider->createGuzzleClientForCurrentTest([
            'base_uri' => $this->client->getApiUrl(),
        ], true);

        $config = (object) [
            'test' => 'neco',
            'array' => [],
            'object' => (object) [],
        ];

        $state = (object) [
            'test' => 'state',
            'array' => [],
            'object' => (object) [
                'subobject' => (object) [],
            ],
        ];

        $response = $client->post('/v2/storage/branch/default/components/wr-db/configs/main-1/rows', [
            'json' => [
                'name' => 'test configuration row',
                'configuration' => json_encode($config),
                'state' => json_encode($state),
                'isDisabled' => $isDisabled,
            ],
            'headers' => [
                'X-StorageApi-Token' => $this->client->getTokenString(),
            ],
        ]);
        $response = json_decode((string) $response->getBody());
        $this->assertEquals($expectedIsDisabled, $response->isDisabled);
        $this->assertEquals('test configuration row', $response->name);
        $this->assertEquals($config, $response->configuration);
        $this->assertEquals($state, $response->state);
    }

    public function isDisabledProvider()
    {
        $providerData = [
            'isDisabled string' => [
                'true',
                true,
            ],
            'isDisabled bool' => [
               true,
               true,
            ] ,
            'isDisabled int' => [
                1,
                true,
            ],
            '!isDisabled string' => [
                'false',
                false,
            ],
            '!isDisabled bool' => [
                false,
                false,
            ],
            '!isDisabled int' => [
                0,
                false,
            ],
        ];

        foreach ([ClientProvider::DEFAULT_BRANCH, ClientProvider::DEV_BRANCH] as $clientType) {
            foreach ($providerData as $providerKey => $provider) {
                yield sprintf('%s: %s', $clientType, $providerKey) => [
                    $clientType,
                    $provider[0],
                    $provider[1],
                ];
            }
        }
    }
}
