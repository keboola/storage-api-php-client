<?php
namespace Keboola\Test\Common;

use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\StorageApi\ProcessPolyfill;
use Keboola\Test\StorageApiTestCase;

class ConfigurationRowTest extends StorageApiTestCase
{
    public function setUp()
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
    }

    /**
     * @dataProvider provideComponentsClient
     */
    public function testConfigurationCopyCreateWithSameRowId(callable $getClient)
    {
        $components = new \Keboola\StorageApi\Components($getClient($this));

        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName("Main 1")
            ->setDescription("description");
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
            ->setName("Main 2")
            ->setDescription("description");
        $components->addConfiguration($config2);

        $rowConfig2 = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config2);
        $rowConfig2->setRowId('main-1-1'); // same rowId to test create correct copy
        $rowConfig2->setState(['key' => 'main-2-1']);
        $components->addConfigurationRow($rowConfig2);

        $copiedConfig = $components->createConfigurationFromVersion(
            'wr-db',
            $config2->getConfigurationId(),
            2,
            'copy-main'
        );
        $response = $components->getConfiguration('wr-db', $copiedConfig["id"]);

        $this->assertSame(['key' => 'main-2-1'], $response['rows'][0]['state']);
    }

    /**
     * @dataProvider provideComponentsClient
     */
    public function testConfigurationRowReturnsSingleRow(callable $getClient)
    {
        $components = new Components($getClient($this));
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
            'main-1-2'
        );

        $this->assertEquals('main-1-2', $row['id']);
    }

    /**
     * @dataProvider provideComponentsClient
     */
    public function testConfigurationRowThrowsNotFoundException(callable $getClient)
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Row invalidRowID not found');

        $components = new Components($getClient($this));
        $configuration = new Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $components->getConfigurationRow(
            'wr-db',
            'main-1',
            'invalidRowID'
        );
    }

    /**
     * @dataProvider provideComponentsClientAndGuzzleClient
     */
    public function testConfigurationRowJsonDataTypes(callable $getClient, callable $getGuzzleClient)
    {
        /** @var Client $_client */
        $_client = $getClient($this);
        if ($_client instanceof BranchAwareClient) {
            $this->markTestIncomplete("Using 'state' parameter on configuration update is restricted for dev/branch context. Use direct API call.");
        }

        $components = new \Keboola\StorageApi\Components($_client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);


        // to check if params is object we have to convert received json to objects instead of assoc array
        // so we have to use raw Http Client
        /** @var \GuzzleHttp\Client $client */
        $client = $getGuzzleClient($this, [
            'base_uri' => $_client->getApiUrl(),
        ], true);

        $config = (object)[
            'test' => 'neco',
            'array' => [],
            'object' => (object)[],
        ];

        $state = (object)[
            'test' => 'state',
            'array' => [],
            'object' => (object)[
                'subobject' => (object)[],
            ]
        ];


        $response = $client->post('/v2/storage/components/wr-db/configs/main-1/rows', [
            'form_params' => [
                'name' => 'test',
                'configuration' => json_encode($config),
                'state' => json_encode($state),
            ],
            'headers' => array(
                'X-StorageApi-Token' => $_client->getTokenString(),
            ),
        ]);
        $response = json_decode((string)$response->getBody());
        $this->assertEquals($config, $response->configuration);
        $this->assertEquals($state, $response->state);

        $response = $client->get('/v2/storage/components/wr-db/configs/main-1/rows/' . $response->id, [
            'headers' => array(
                'X-StorageApi-Token' => $_client->getTokenString(),
            ),
        ]);
        $response = json_decode((string)$response->getBody());
        $this->assertEquals($config, $response->configuration);
        $this->assertEquals($state, $response->state);

        // update
        $config = (object)[
            'test' => 'neco',
            'array' => ['2'],
            'anotherArr' => [],
            'object' => (object)[],
        ];
        $state = (object)[
            'test2' => 'state',
            'array2' => [],
            'object2' => (object)[
                'subobject2' => (object)[],
            ]
        ];


        $response = $client->put('/v2/storage/components/wr-db/configs/main-1/rows/' . $response->id, [
            'form_params' => [
                'configuration' => json_encode($config),
                'state' => json_encode($state),
            ],
            'headers' => array(
                'X-StorageApi-Token' => $_client->getTokenString(),
            ),
        ]);
        $response = json_decode((string)$response->getBody());
        $this->assertEquals($config, $response->configuration);
        $this->assertEquals($state, $response->state);

        $response = $client->get('/v2/storage/components/wr-db/configs/main-1/rows/' . $response->id, [
            'headers' => array(
                'X-StorageApi-Token' => $_client->getTokenString(),
            ),
        ]);
        $response = json_decode((string)$response->getBody());
        $this->assertEquals($config, $response->configuration);
        $this->assertEquals($state, $response->state);
        $this->assertFalse($response->isDisabled);
    }

    /**
     * @dataProvider provideComponentsClientAndGuzzleClient
     */
    public function testConfigurationRowIsDisabledBooleanValue(callable $getClient, callable $getGuzzleClient)
    {
        /** @var Client $_client */
        $_client = $getClient($this);

        $components = new \Keboola\StorageApi\Components($_client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $client = $getGuzzleClient($this, [
            'base_uri' => $this->_client->getApiUrl(),
        ], true);

        $config = (object)[
            'test' => 'neco',
            'array' => [],
            'object' => (object)[],
        ];

        $state = (object)[
            'test' => 'state',
            'array' => [],
            'object' => (object)[
                'subobject' => (object)[],
            ]
        ];

        $response = $client->post('/v2/storage/components/wr-db/configs/main-1/rows', [
            'form_params' => [
                'configuration' => json_encode($config),
                'state' => json_encode($state),
            ],
            'headers' => array(
                'X-StorageApi-Token' => $this->_client->getTokenString(),
            ),
        ]);
        $response = json_decode((string) $response->getBody());

        $responsePut = $client->put("/v2/storage/components/wr-db/configs/main-1/rows/" . $response->id, [
            'form_params' => [
                'isDisabled' => 'true',
                'changeDescription' => 'Row ABCD disabled',
            ],
            'headers' => [
                'X-StorageApi-Token' => $this->_client->getTokenString(),
            ],
        ]);

        $result = json_decode((string) $responsePut->getBody());
        $this->assertTrue($result->isDisabled);
        $this->assertEquals("Row ABCD disabled", $result->changeDescription);
    }

    public function provideComponentsClientAndGuzzleClient()
    {
        $clients = [];
        foreach ($this->provideComponentsClient() as $name => $callable) {
            $clients[$name] = reset($callable);
        }

        $guzzleClients = [];
        foreach ($this->provideComponentsGuzzleClient() as $name => $callable) {
            $guzzleClients[$name] = reset($callable);
        }

        yield 'defaultBranch' => [
            $clients['defaultBranch'],
            $guzzleClients['defaultBranch'],
        ];

        yield 'devBranch' => [
            $clients['devBranch'],
            $guzzleClients['devBranch'],
        ];
    }

    /**
     * @dataProvider isDisabledProvider
     */
    public function testCreateConfigurationRowIsDisabled(callable $getClient, callable $getGuzzleClient, $isDisabled, $expectedIsDisabled)
    {
        /** @var Client $_client */
        $_client = $getClient($this);

        $components = new \Keboola\StorageApi\Components($_client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $client = $getGuzzleClient($this, [
            'base_uri' => $this->_client->getApiUrl(),
        ], true);

        $config = (object)[
            'test' => 'neco',
            'array' => [],
            'object' => (object)[],
        ];

        $state = (object)[
            'test' => 'state',
            'array' => [],
            'object' => (object)[
                'subobject' => (object)[],
            ]
        ];

        $response = $client->post('/v2/storage/components/wr-db/configs/main-1/rows', [
            'form_params' => [
                'name' => 'test configuration row',
                'configuration' => json_encode($config),
                'state' => json_encode($state),
                'isDisabled' => $isDisabled,
            ],
            'headers' => array(
                'X-StorageApi-Token' => $_client->getTokenString(),
            ),
        ]);
        $response = json_decode((string)$response->getBody());
        $this->assertEquals($expectedIsDisabled, $response->isDisabled);
        $this->assertEquals('test configuration row', $response->name);
        $this->assertEquals($config, $response->configuration);
        $this->assertEquals($state, $response->state);
    }

    public function isDisabledProvider()
    {
        $clients = [];
        foreach ($this->provideComponentsClient() as $name => $callable) {
            $clients[$name] = reset($callable);
        }

        $guzzleClients = [];
        foreach ($this->provideComponentsGuzzleClient() as $name => $callable) {
            $guzzleClients[$name] = reset($callable);
        }

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
                false
            ],
            '!isDisabled int' => [
                0,
                false,
            ],
        ];

        foreach (['defaultBranch', 'devBranch'] as $branch) {
            foreach ($providerData as $providerKey => $provider) {
                yield sprintf('%s: %s', $branch, $providerKey) => [
                    $clients[$branch],
                    $guzzleClients[$branch],
                    $provider[0],
                    $provider[1],
                ];
            }
        }
    }
}
