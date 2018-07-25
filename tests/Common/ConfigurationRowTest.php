<?php
namespace Keboola\Test\Common;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions;
use Keboola\Test\StorageApiTestCase;
use Symfony\Component\Process\Process;

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

    public function testConfigurationRowReturnsSingleRow()
    {
        $components = new Components($this->_client);
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

        # qwe
        $row = $components->getConfigurationRow(
            (new ListConfigurationRowsOptions())
                ->setComponentId('wr-db')
                ->setConfigurationId('main-1')
                ->setRowId('main-1-2')
        );

        $this->assertEquals('main-1-2', $row['id']);
    }

    public function testConfigurationRowThrowsNotFoundException()
    {
        $components = new Components($this->_client);
        $configuration = new Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $this->expectException(ClientException::class);
        $components->getConfigurationRow(
            (new ListConfigurationRowsOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setRowId(666)
        );
    }

    public function testConfigurationRowJsonDataTypes()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);


        // to check if params is object we have to convert received json to objects instead of assoc array
        // so we have to use raw Http Client
        $client = new \GuzzleHttp\Client([
            'base_uri' => $this->_client->getApiUrl(),
        ]);

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
                'X-StorageApi-Token' => $this->_client->getTokenString(),
            ),
        ]);
        $response = json_decode((string)$response->getBody());
        $this->assertEquals($config, $response->configuration);
        $this->assertEquals($state, $response->state);

        $response = $client->get('/v2/storage/components/wr-db/configs/main-1/rows/' . $response->id, [
            'headers' => array(
                'X-StorageApi-Token' => $this->_client->getTokenString(),
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
                'X-StorageApi-Token' => $this->_client->getTokenString(),
            ),
        ]);
        $response = json_decode((string)$response->getBody());
        $this->assertEquals($config, $response->configuration);
        $this->assertEquals($state, $response->state);

        $response = $client->get('/v2/storage/components/wr-db/configs/main-1/rows/' . $response->id, [
            'headers' => array(
                'X-StorageApi-Token' => $this->_client->getTokenString(),
            ),
        ]);
        $response = json_decode((string)$response->getBody());
        $this->assertEquals($config, $response->configuration);
        $this->assertEquals($state, $response->state);
    }

    public function testConfigurationRowIsDisabledBooleanValue()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $client = new \GuzzleHttp\Client([
            'base_uri' => $this->_client->getApiUrl(),
        ]);

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
        $response = json_decode((string)$response->getBody());

        $command = "curl '" . STORAGE_API_URL . "/v2/storage/components/wr-db/configs/main-1/rows/{$response->id}' \
                    -X PUT \
                    -H 'accept-encoding: gzip, deflate, br' \
                    -H 'accept-language: en-US,en;q=0.9,de;q=0.8,sk;q=0.7' \
                    -H 'content-type: application/x-www-form-urlencoded' \
                    -H 'accept: */*' \
                    -H 'x-storageapi-token: " . STORAGE_API_TOKEN . "' \
                    --data 'isDisabled=true&changeDescription=Row%20ABCD%20disabled' \
                    --compressed";

        $process = new Process($command);
        $process->run();
        if (!$process->isSuccessful()) {
            $this->fail("Config Row PUT request should not produce an error.");
        }

        $result = json_decode($process->getOutput());
        $this->assertTrue($result->isDisabled);
        $this->assertEquals("Row ABCD disabled", $result->changeDescription);
    }
}
