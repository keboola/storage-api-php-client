<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\Test\Common;

use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\Test\StorageApiTestCase;
use Symfony\Component\Process\Process;

class ComponentsTest extends StorageApiTestCase
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

    public function testComponentConfigRenew()
    {
        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $components = new \Keboola\StorageApi\Components($this->_client);

        $configuration = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setDescription('some desc');
        $components->addConfiguration($configuration);
        $components->addConfigurationRow((new ConfigurationRow($configuration))
            ->setRowId('firstRow')
            ->setConfiguration(['value' => 1]));
        $components->deleteConfiguration($componentId, $configurationId);

        // create configuration with same id as deleted
        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setConfiguration(["test" => false])
            ->setName('Main renewed')
            ->setDescription('some desc for renew'));

        $this->assertCount(0, $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)->setIsDeleted(true)
        ));

        $componentList = $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        );
        $this->assertCount(1, $componentList);

        $component = reset($componentList);
        $this->assertEquals($configurationId, $component['id']);
        $this->assertEquals('Main renewed', $component['name']);
        $this->assertEquals('some desc for renew', $component['description']);
        $this->assertEquals(["test" => false], $component['configuration']);
        $this->assertSame('Configuration created', $component['changeDescription']);
        $this->assertFalse($component['isDeleted']);
        $this->assertEquals(4, $component['version']);
        $this->assertInternalType('int', $component['version']);
        $this->assertInternalType('int', $component['creatorToken']['id']);
        $this->assertCount(0, $component['rows']);
    }

    public function testComponentConfigDelete()
    {
        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $components = new \Keboola\StorageApi\Components($this->_client);

        $this->assertCount(0, $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        ));
        $this->assertCount(0, $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)->setIsDeleted(true)
        ));

        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setDescription('some desc'));

        $component = $components->getConfiguration($componentId, $configurationId);
        $this->assertEquals('Main', $component['name']);
        $this->assertEquals('some desc', $component['description']);
        $this->assertEmpty($component['configuration']);
        $this->assertSame('Configuration created', $component['changeDescription']);
        $this->assertFalse($component['isDeleted']);
        $this->assertEquals(1, $component['version']);
        $this->assertInternalType('int', $component['version']);
        $this->assertInternalType('int', $component['creatorToken']['id']);

        $components->deleteConfiguration($componentId, $configurationId);

        $this->assertCount(0, $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        ));
        $this->assertCount(0, $components->listComponents());

        // test that sending string 'false' for isDeleted is supported https://github.com/keboola/connection/issues/1047
        $command = "curl '" . STORAGE_API_URL . "/v2/storage/components/{$componentId}/configs?isDeleted=false' \
                    -X GET \
                    -H 'content-type: application/x-www-form-urlencoded' \
                    -H 'accept: */*' \
                    -H 'x-storageapi-token: " . STORAGE_API_TOKEN . "'";
        $process = new Process($command);
        $process->run();
        if (!$process->isSuccessful()) {
            $this->fail("Api request failure GET component configs");
        }
        $result = json_decode($process->getOutput(), true);
        $this->assertCount(0, $result);

        // test that sending string 'false' for isDeleted is supported https://github.com/keboola/connection/issues/1047
        $command = "curl '" . STORAGE_API_URL . "/v2/storage/components?isDeleted=false' \
                    -X GET \
                    -H 'content-type: application/x-www-form-urlencoded' \
                    -H 'accept: */*' \
                    -H 'x-storageapi-token: " . STORAGE_API_TOKEN . "'";
        $process = new Process($command);
        $process->run();
        if (!$process->isSuccessful()) {
            $this->fail("Api request failure GET component list");
        }
        $result = json_decode($process->getOutput(), true);
        $this->assertCount(0, $result);

        $componentList = $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)->setIsDeleted(true)
        );
        $this->assertCount(1, $componentList);

        $component = reset($componentList);
        $this->assertEquals($configurationId, $component['id']);
        $this->assertEquals('Main', $component['name']);
        $this->assertEquals('some desc', $component['description']);
        $this->assertNotEmpty($component['changeDescription']);
        $this->assertTrue($component['isDeleted']);
        $this->assertEquals(2, $component['version']);
        $this->assertInternalType('int', $component['version']);
        $this->assertInternalType('int', $component['creatorToken']['id']);

        $currentVersion = $component['currentVersion'];
        $this->assertEquals('Configuration deleted', $currentVersion['changeDescription']);
        $tokenInfo = $this->_client->verifyToken();
        $this->assertEquals($tokenInfo['id'], $currentVersion['creatorToken']['id']);
        $this->assertEquals($tokenInfo['description'], $currentVersion['creatorToken']['description']);

        $componentsIndex = $components->listComponents((new ListComponentsOptions())->setIsDeleted(true));

        $this->assertCount(1, $componentsIndex);
        $this->assertArrayHasKey('id', $componentsIndex[0]);
        $this->assertArrayHasKey('configurations', $componentsIndex[0]);
        $this->assertEquals($componentId, $componentsIndex[0]['id']);
        $this->assertCount(1, $componentsIndex[0]['configurations']);

        $components->deleteConfiguration($componentId, $configurationId);

        $this->assertCount(0, $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        ));
        $this->assertCount(0, $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)->setIsDeleted(true)
        ));
    }

    public function testComponentConfigRestore()
    {
        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $components = new \Keboola\StorageApi\Components($this->_client);

        $this->assertCount(0, $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        ));
        $this->assertCount(0, $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)->setIsDeleted(true)
        ));

        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setDescription('some desc'));

        $component = $components->getConfiguration($componentId, $configurationId);
        $this->assertEquals('Main', $component['name']);
        $this->assertEquals('some desc', $component['description']);
        $this->assertEmpty($component['configuration']);
        $this->assertSame('Configuration created', $component['changeDescription']);
        $this->assertFalse($component['isDeleted']);
        $this->assertEquals(1, $component['version']);
        $this->assertInternalType('int', $component['version']);
        $this->assertInternalType('int', $component['creatorToken']['id']);

        $components->deleteConfiguration($componentId, $configurationId);

        $this->assertCount(0, $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        ));

        $componentList = $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)->setIsDeleted(true)
        );
        $this->assertCount(1, $componentList);

        $component = reset($componentList);
        $this->assertEquals($configurationId, $component['id']);
        $this->assertEquals('Main', $component['name']);
        $this->assertEquals('some desc', $component['description']);
        $this->assertNotEmpty('Configuration deleted');
        $this->assertTrue($component['isDeleted']);
        $this->assertEquals(2, $component['version']);
        $this->assertInternalType('int', $component['version']);
        $this->assertInternalType('int', $component['creatorToken']['id']);

        $components->restoreComponentConfiguration($componentId, $configurationId);

        $this->assertCount(0, $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)->setIsDeleted(true)
        ));

        $componentList = $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        );
        $this->assertCount(1, $componentList);

        $component = reset($componentList);
        $this->assertEquals($configurationId, $component['id']);
        $this->assertEquals('Main', $component['name']);
        $this->assertEquals('some desc', $component['description']);
        $this->assertNotEmpty('Configuration restored');
        $this->assertFalse($component['isDeleted']);
        $this->assertEquals(3, $component['version']);
        $this->assertInternalType('int', $component['version']);
        $this->assertInternalType('int', $component['creatorToken']['id']);
    }

    public function testComponentConfigCreate()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc'));

        $component = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals('Main', $component['name']);
        $this->assertEquals('some desc', $component['description']);
        $this->assertEmpty($component['configuration']);
        $this->assertSame('Configuration created', $component['changeDescription']);
        $this->assertEquals(1, $component['version']);
        $this->assertInternalType('int', $component['version']);
        $this->assertInternalType('int', $component['creatorToken']['id']);

        $components = $components->listComponents();
        $this->assertCount(1, $components);

        $component = reset($components);
        $this->assertEquals('wr-db', $component['id']);
        $this->assertCount(1, $component['configurations']);

        $configuration = reset($component['configurations']);
        $this->assertEquals('main-1', $configuration['id']);
        $this->assertEquals('Main', $configuration['name']);
        $this->assertEquals('some desc', $configuration['description']);
    }

    public function testConfigurationNameShouldBeRequired()
    {
        try {
            $this->_client->apiPost('storage/components/wr-db/configs', []);
            $this->fail('Params should be invalid');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.components.validation', $e->getStringCode());
            $this->assertContains('name', $e->getMessage());
        }
    }

    public function testConfigurationDescriptionDefault()
    {
        $resp = $this->_client->apiPost('storage/components/wr-db/configs', [
            'name' => 'neco'
        ]);
        $components = new \Keboola\StorageApi\Components($this->_client);
        $configuration = $components->getConfiguration('wr-db', $resp['id']);
        $this->assertNotNull($configuration['description']);
    }

    public function testNonJsonConfigurationShouldNotBeAllowed()
    {
        try {
            $this->_client->apiPost('storage/components/wr-db/configs', array(
                'name' => 'neco',
                'description' => 'some',
                'configuration' => '{sdf}',
            ));
            $this->fail('Post invalid json should not be allowed.');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('validation.invalidConfigurationFormat', $e->getStringCode());
        }
    }

    public function testComponentConfigurationJsonDataTypes()
    {
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


        $response = $client->post("/v2/storage/components/wr-db/configs", [
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

        $response = $client->get("/v2/storage/components/wr-db/configs/{$response->id}", [
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
        $response = $client->put("/v2/storage/components/wr-db/configs/{$response->id}", [
            'form_params' => [
                'configuration' => json_encode($config),
            ],
            'headers' => array(
                'X-StorageApi-Token' => $this->_client->getTokenString(),
            ),
        ]);
        $response = json_decode((string)$response->getBody());
        $this->assertEquals($config, $response->configuration);

        $response = $client->get("/v2/storage/components/wr-db/configs/{$response->id}", [
            'headers' => array(
                'X-StorageApi-Token' => $this->_client->getTokenString(),
            ),
        ]);
        $response = json_decode((string)$response->getBody());
        $this->assertEquals($config, $response->configuration);
    }

    public function testComponentConfigCreateWithConfigurationJson()
    {
        $configuration = array(
            'queries' => array(
                array(
                    'id' => 1,
                    'query' => 'SELECT * from some_table',
                ),
            ),
        );

        $components = new \Keboola\StorageApi\Components($this->_client);
        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc')
            ->setConfiguration($configuration));

        $config = $components->getConfiguration('wr-db', 'main-1');

        $this->assertEquals($configuration, $config['configuration']);
        $this->assertEquals(1, $config['version']);
    }

    public function testComponentConfigCreateWithStateJson()
    {
        $state = array(
            'queries' => array(
                array(
                    'id' => 1,
                    'query' => 'SELECT * from some_table',
                )
            ),
        );
        $components = new \Keboola\StorageApi\Components($this->_client);
        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc')
            ->setState($state));

        $config = $components->getConfiguration('wr-db', 'main-1');

        $this->assertEquals($state, $config['state']);
        $this->assertEquals(1, $config['version']);
    }

    public function testComponentConfigUpdateEmptyStateJson()
    {
        $state = array(
            'queries' => array(
                array(
                    'id' => 1,
                    'query' => 'SELECT * from some_table',
                )
            ),
        );
        $components = new \Keboola\StorageApi\Components($this->_client);
        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc')
            ->setState($state));

        $config = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals($state, $config['state']);
        $this->assertEquals(1, $config['version']);
        $components->updateConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc')
            ->setState([]));

        $config = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals([], $config['state']);
        $this->assertEquals(1, $config['version']);
    }

    public function testComponentConfigCreateIdAutoCreate()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $component = $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setName('Main')
            ->setDescription('some desc'));
        $this->assertNotEmpty($component['id']);
        $component = $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setName('Main')
            ->setDescription('some desc'));
        $this->assertNotEmpty($component['id']);
    }

    public function testComponentConfigUpdate()
    {
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components = new \Keboola\StorageApi\Components($this->_client);
        $newConfiguration = $components->addConfiguration($config);
        $this->assertEquals(1, $newConfiguration['version']);
        $this->assertEmpty($newConfiguration['state']);

        $newName = 'neco';
        $newDesc = 'some desc';
        $configurationData = array('x' => 'y');
        $config->setName($newName)
            ->setDescription($newDesc)
            ->setConfiguration($configurationData);
        $components->updateConfiguration($config);

        $configuration = $components->getConfiguration($config->getComponentId(), $config->getConfigurationId());

        $this->assertEquals($newName, $configuration['name']);
        $this->assertEquals($newDesc, $configuration['description']);
        $this->assertEquals($config->getConfiguration(), $configuration['configuration']);
        $this->assertEquals(2, $configuration['version']);
        $this->assertEmpty($configuration['changeDescription']);

        $state = [
            'cache' => true,
        ];
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setDescription('neco')
            ->setState($state);

        $updatedConfig = $components->updateConfiguration($config);
        $this->assertEquals($newName, $updatedConfig['name'], 'Name should not be changed after description update');
        $this->assertEquals('neco', $updatedConfig['description']);
        $this->assertEquals($configurationData, $updatedConfig['configuration']);
        $this->assertEquals($state, $updatedConfig['state']);
        $this->assertEmpty($updatedConfig['changeDescription']);

        $configuration = $components->getConfiguration($config->getComponentId(), $config->getConfigurationId());

        $this->assertEquals($newName, $configuration['name'], 'Name should not be changed after description update');
        $this->assertEquals('neco', $configuration['description']);
        $this->assertEquals($configurationData, $configuration['configuration']);
        $this->assertEquals($state, $configuration['state']);
        $this->assertEmpty($configuration['changeDescription']);

        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setDescription('');

        $components->updateConfiguration($config);
        $configuration = $components->getConfiguration($config->getComponentId(), $config->getConfigurationId());
        $this->assertEquals('', $configuration['description'], 'Description can be set empty');
    }


    public function testComponentConfigUpdateConfigEmpty()
    {
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components = new \Keboola\StorageApi\Components($this->_client);
        $newConfiguration = $components->addConfiguration($config);
        $this->assertEquals(1, $newConfiguration['version']);
        $this->assertEmpty($newConfiguration['state']);

        $newName = 'neco';
        $newDesc = 'some desc';
        $configurationData = array('foo' => 'bar');
        $config->setName($newName)
            ->setDescription($newDesc)
            ->setConfiguration($configurationData);
        $components->updateConfiguration($config);

        $configuration = $components->getConfiguration($config->getComponentId(), $config->getConfigurationId());

        $this->assertEquals($config->getConfiguration(), $configuration['configuration']);
        $this->assertEquals(2, $configuration['version']);

        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setConfiguration([])
        ;

        $updatedConfig = $components->updateConfiguration($config);
        $this->assertEquals([], $updatedConfig['configuration']);
        $this->assertEmpty($updatedConfig['changeDescription']);
        $this->assertEquals(3, $updatedConfig['version']);
    }

    public function testComponentConfigUpdateEmptyWithEmpty()
    {
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components = new \Keboola\StorageApi\Components($this->_client);
        $newConfiguration = $components->addConfiguration($config);
        $this->assertEquals(1, $newConfiguration['version']);
        $this->assertEmpty($newConfiguration['configuration']);

        $config->setConfiguration([]);
        $components->updateConfiguration($config);

        $updatedConfig = $components->getConfiguration($config->getComponentId(), $config->getConfigurationId());
        $this->assertEquals(1, $updatedConfig['version'], 'there should not be any change');

        $components->updateConfiguration($config);
        $updatedConfig = $components->getConfiguration($config->getComponentId(), $config->getConfigurationId());
        $this->assertEquals(1, $updatedConfig['version'], 'there should not be any change');
    }

    public function testComponentConfigUpdateWithRows()
    {
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components = new \Keboola\StorageApi\Components($this->_client);
        $newConfiguration = $components->addConfiguration($config);
        $this->assertEquals(1, $newConfiguration['version']);
        $this->assertEmpty($newConfiguration['state']);

        $newName = 'neco';
        $newDesc = 'some desc';
        $configurationData = array('x' => 'y');
        $config->setName($newName)
            ->setDescription($newDesc)
            ->setConfiguration($configurationData);
        $components->updateConfiguration($config);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $configurationRow->setRowId('main-1-1');

        $components->addConfigurationRow($configurationRow);

        $configuration = $components->getConfiguration($config->getComponentId(), $config->getConfigurationId());

        $this->assertEquals($newName, $configuration['name']);
        $this->assertEquals($newDesc, $configuration['description']);
        $this->assertEquals($config->getConfiguration(), $configuration['configuration']);
        $this->assertEquals(3, $configuration['version']);

        $this->assertArrayHasKey('rows', $configuration);
        $this->assertCount(1, $configuration['rows']);

        $row = reset($configuration['rows']);
        $this->assertEquals('main-1-1', $row['id']);

        $state = [
            'cache' => true,
        ];
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setDescription('neco')
            ->setState($state);

        $updatedConfig = $components->updateConfiguration($config);
        $this->assertEquals($newName, $updatedConfig['name'], 'Name should not be changed after description update');
        $this->assertEquals('neco', $updatedConfig['description']);
        $this->assertEquals($configurationData, $updatedConfig['configuration']);
        $this->assertEquals($state, $updatedConfig['state']);

        $this->assertArrayHasKey('rows', $updatedConfig);
        $this->assertCount(1, $updatedConfig['rows']);

        $row = reset($updatedConfig['rows']);
        $this->assertEquals('main-1-1', $row['id']);

        $configuration = $components->getConfiguration($config->getComponentId(), $config->getConfigurationId());

        $this->assertEquals($newName, $configuration['name'], 'Name should not be changed after description update');
        $this->assertEquals('neco', $configuration['description']);
        $this->assertEquals($configurationData, $configuration['configuration']);
        $this->assertEquals($state, $configuration['state']);

        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setDescription('');

        $components->updateConfiguration($config);
        $configuration = $components->getConfiguration($config->getComponentId(), $config->getConfigurationId());
        $this->assertEquals('', $configuration['description'], 'Description can be set empty');

        $this->assertArrayHasKey('rows', $configuration);
        $this->assertCount(1, $configuration['rows']);

        $row = reset($configuration['rows']);
        $this->assertEquals('main-1-1', $row['id']);
    }

    public function testComponentConfigUpdateVersioning()
    {
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components = new \Keboola\StorageApi\Components($this->_client);
        $newConfiguration = $components->addConfiguration($config);
        $this->assertEquals(1, $newConfiguration['version']);
        $this->assertEmpty($newConfiguration['state']);

        $listConfig = (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
            ->setComponentId($config->getComponentId())
            ->setConfigurationId($config->getConfigurationId())
            ->setInclude(array('name', 'state'));
        $versions = $components->listConfigurationVersions($listConfig);
        $this->assertCount(1, $versions, 'Configuration should have one version');

        $newName = 'neco';
        $newDesc = 'some desc';
        $configurationData = array('x' => 'y');
        $config->setName($newName)
            ->setDescription($newDesc)
            ->setConfiguration($configurationData);
        $components->updateConfiguration($config);
        $versions = $components->listConfigurationVersions($listConfig);
        $this->assertCount(2, $versions, 'Update of configuration name should add version');
        $lastVersion = reset($versions);
        $this->assertEquals(2, $lastVersion['version']);

        $state = ['cache' => true];
        $config->setState($state);
        $components->updateConfiguration($config);
        $versions = $components->listConfigurationVersions($listConfig);
        $this->assertCount(2, $versions, 'Update of configuration state should not add version');
        $lastVersion = reset($versions);
        $this->assertEquals(2, $lastVersion['version']);

        $components->updateConfiguration($config);
        $versions = $components->listConfigurationVersions($listConfig);
        $this->assertCount(2, $versions, 'Update without change should not add version');
        $lastVersion = reset($versions);
        $this->assertEquals(2, $lastVersion['version']);
    }

    public function testComponentConfigUpdateChangeDescription()
    {
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components = new \Keboola\StorageApi\Components($this->_client);
        $newConfiguration = $components->addConfiguration($config);
        $this->assertEquals(1, $newConfiguration['version']);
        $this->assertEmpty($newConfiguration['state']);

        $changeDesc = 'change Description';
        $newName = 'neco';
        $newDesc = 'some desc';
        $configurationData = array('x' => 'y');
        $config->setName($newName)
            ->setDescription($newDesc)
            ->setConfiguration($configurationData)
            ->setChangeDescription($changeDesc);
        $components->updateConfiguration($config);

        $componentConfig = $components->getConfiguration('wr-db', 'main-1');
        $this->assertArrayHasKey('changeDescription', $componentConfig);
        $this->assertEquals($changeDesc, $componentConfig['changeDescription']);

        $listConfig = (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
            ->setComponentId($config->getComponentId())
            ->setConfigurationId($config->getConfigurationId())
            ->setInclude(array('name', 'state'));
        $versions = $components->listConfigurationVersions($listConfig);
        $this->assertArrayHasKey('changeDescription', $versions[0]);
        $this->assertEquals($changeDesc, $versions[0]['changeDescription']);

        // change name without providing changeDescription param
        $secondConfigToPut = (new \Keboola\StorageApi\Options\Components\Configuration());
        $secondConfigToPut->setComponentId('wr-db');
        $secondConfigToPut->setConfigurationId('main-1');
        $secondConfigToPut->setName('new name');
        $components->updateConfiguration($secondConfigToPut);

        $secondConfigLoaded = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals('', $secondConfigLoaded['changeDescription']);
    }

    public function testComponentConfigsVersionsList()
    {
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components = new \Keboola\StorageApi\Components($this->_client);
        $newConfiguration = $components->addConfiguration($config);
        $this->assertEquals(1, $newConfiguration['version']);
        $this->assertEmpty($newConfiguration['state']);
        $this->assertEquals($newConfiguration['changeDescription'], $newConfiguration['currentVersion']['changeDescription']);
        $this->assertEquals($newConfiguration['creatorToken'], $newConfiguration['currentVersion']['creatorToken']);

        $newName = 'neco';
        $newDesc = 'some desc';
        $configurationData = array('x' => 'y');
        $config->setName($newName)
            ->setDescription($newDesc)
            ->setConfiguration($configurationData);
        $components->updateConfiguration($config);

        $configuration = $components->getConfiguration($config->getComponentId(), $config->getConfigurationId());
        $this->assertEquals(2, $configuration['version']);

        $config = (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
            ->setComponentId($config->getComponentId())
            ->setConfigurationId($config->getConfigurationId())
            ->setInclude(array('name', 'state'));
        $result = $components->listConfigurationVersions($config);
        $this->assertCount(2, $result);
        $this->assertArrayHasKey('version', $result[0]);
        $this->assertEquals(2, $result[0]['version']);
        $this->assertArrayHasKey('name', $result[0]);
        $this->assertEquals('neco', $result[0]['name']);
        $this->assertArrayNotHasKey('state', $result[0]);
        $this->assertArrayNotHasKey('description', $result[0]);
        $this->assertArrayHasKey('version', $result[1]);
        $this->assertEquals(1, $result[1]['version']);
        $this->assertArrayHasKey('name', $result[1]);
        $this->assertEquals('Main', $result[1]['name']);
        $this->assertEquals($result[0]['changeDescription'], $configuration['currentVersion']['changeDescription']);
        $this->assertEquals($result[0]['creatorToken'], $configuration['currentVersion']['creatorToken']);
        $this->assertEquals($result[0]['created'], $configuration['currentVersion']['created']);

        $config = (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
            ->setComponentId($config->getComponentId())
            ->setConfigurationId($config->getConfigurationId())
            ->setInclude(array('name', 'configuration'))
            ->setOffset(0)
            ->setLimit(1);
        $result = $components->listConfigurationVersions($config);
        $this->assertCount(1, $result);
        $this->assertArrayHasKey('version', $result[0]);
        $this->assertEquals(2, $result[0]['version']);
        $this->assertArrayNotHasKey('state', $result[0]);
        $this->assertArrayHasKey('configuration', $result[0]);
        $this->assertEquals($configurationData, $result[0]['configuration']);

        $config = (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
            ->setComponentId($config->getComponentId())
            ->setConfigurationId($config->getConfigurationId());
        $result = $components->getConfigurationVersion($config->getComponentId(), $config->getConfigurationId(), 2);
        $this->assertArrayHasKey('version', $result);
        $this->assertInternalType('int', $result['version']);
        $this->assertEquals(2, $result['version']);
        $this->assertInternalType('int', $result['creatorToken']['id']);
        $this->assertArrayNotHasKey('state', $result);
        $this->assertArrayHasKey('configuration', $result);
        $this->assertEquals($configurationData, $result['configuration']);
        $result = $components->listConfigurationVersions($config);
        $this->assertCount(2, $result);
    }

    /**
     * Create configuration with few rows, update some row and then rollback to configuration with updated row
     */
    public function testConfigurationRollback()
    {
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setConfiguration(['a' => 'b'])
            ->setName('Main');
        $components = new \Keboola\StorageApi\Components($this->_client);
        $newConfiguration = $components->addConfiguration($config);


        // add first row
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $firstRowConfig = array('first' => 1);
        $configurationRow->setConfiguration($firstRowConfig);
        $firstRow = $components->addConfigurationRow($configurationRow);

        $secondVersion = $components->getConfiguration('wr-db', $newConfiguration['id']);
        $this->assertEquals(2, $secondVersion['version']);
        $this->assertEquals($firstRowConfig, $secondVersion['rows'][0]['configuration']);
        $this->assertEquals(sprintf('Row %s added', $firstRow['id']), $secondVersion['changeDescription']);


        // add another row
        $secondRowConfig = array('second' => 1);
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $configurationRow->setConfiguration($secondRowConfig);
        $secondRow = $components->addConfigurationRow($configurationRow);

        // update first row
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $firstRowUpdatedConfig = array('first' => 22);
        $configurationRow->setConfiguration($firstRowUpdatedConfig)->setRowId($firstRow['id']);
        $components->updateConfigurationRow($configurationRow);

        // update config
        $components->updateConfiguration($config->setConfiguration(['d' => 'b']));

        $expectedRows = [
            [
                'id' => $firstRow['id'],
                'version' => 2,
                'configuration' => $firstRowUpdatedConfig,
            ],
            [
                'id' => $secondRow['id'],
                'version' => 1,
                'configuration' => $secondRowConfig,
            ]
        ];

        $currentConfiguration = $components->getConfiguration('wr-db', $newConfiguration['id']);
        $this->assertEquals(5, $currentConfiguration['version'], 'There were 2 rows insert and 1 row update and 1 config update -> version should be 5');
        foreach ($expectedRows as $i => $expectedRow) {
            $this->assertEquals($expectedRow['id'], $currentConfiguration['rows'][$i]['id']);
            $this->assertEquals($expectedRow['version'], $currentConfiguration['rows'][$i]['version']);
            $this->assertEquals($expectedRow['configuration'], $currentConfiguration['rows'][$i]['configuration']);
        }

        // rollback to version 2
        // second row should be missing, and first row should be rolled back to first version
        $components->rollbackConfiguration('wr-db', $newConfiguration['id'], 2);

        $currentConfiguration = $components->getConfiguration('wr-db', $newConfiguration['id']);
        $this->assertEquals(6, $currentConfiguration['version'], 'Rollback was one new operation');

        $this->assertCount(1, $currentConfiguration['rows']);

        $row = reset($currentConfiguration['rows']);
        $this->assertEquals($firstRow['id'], $row['id']);
        $this->assertEquals(3, $row['version']);
        $this->assertEquals($newConfiguration['created'], $currentConfiguration['created']);
        $this->assertEquals($firstRowConfig, $row['configuration']);
        $this->assertEquals(['a' => 'b'], $currentConfiguration['configuration']);
        $this->assertEquals("Rollback from version 2", $currentConfiguration['changeDescription']);
    }

    public function testUpdateRowWithoutIdShouldNotBeAllowed()
    {
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components = new \Keboola\StorageApi\Components($this->_client);
        $newConfiguration = $components->addConfiguration($config);

        // add first row
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $firstRowConfig = array('first' => 1);
        $configurationRow->setConfiguration($firstRowConfig);
        $firstRow = $components->addConfigurationRow($configurationRow);

        // update row
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $configurationRow->setConfiguration(['first' => 'dd']);
        try {
            $components->updateConfigurationRow($configurationRow);
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(400, $e->getCode(), 'User error should be thrown');
        }
    }

    public function testUpdateConfigWithoutIdShouldNotBeAllowed()
    {
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components = new \Keboola\StorageApi\Components($this->_client);
        $newConfiguration = $components->addConfiguration($config);

        $config->setConfigurationId(null);

        try {
            $components->updateConfiguration($config);
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(400, $e->getCode(), 'User error should be thrown');
        }
    }


    public function testComponentConfigsVersionsRollback()
    {
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components = new \Keboola\StorageApi\Components($this->_client);
        $newConfiguration = $components->addConfiguration($config);
        $this->assertEquals(1, $newConfiguration['version']);
        $this->assertEmpty($newConfiguration['state']);


        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $configurationRow->setRowId('main-1-1')
            ->setConfiguration(array('first' => 1));

        $components->addConfigurationRow($configurationRow);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $configurationRow->setRowId('main-1-2')
            ->setConfiguration(array('second' => 1));

        $components->addConfigurationRow($configurationRow);

        $listOptions = new \Keboola\StorageApi\Options\Components\ListComponentsOptions();
        $listOptions->setInclude(array('rows'));
        $components = $components->listComponents($listOptions);

        $this->assertCount(1, $components);

        $component = reset($components);
        $configuration = reset($component['configurations']);

        $this->assertArrayHasKey('rows', $configuration);
        $this->assertCount(2, $configuration['rows']);

        $components = new \Keboola\StorageApi\Components($this->_client);

        $newName = 'neco';
        $newDesc = 'some desc';
        $configurationData = array('x' => 'y');
        $config->setName($newName)
            ->setDescription($newDesc)
            ->setConfiguration($configurationData);
        $components->updateConfiguration($config);

        $config = (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
            ->setComponentId($config->getComponentId())
            ->setConfigurationId($config->getConfigurationId());
        $result = $components->rollbackConfiguration($config->getComponentId(), $config->getConfigurationId(), 2);
        $this->assertArrayHasKey('version', $result);
        $this->assertEquals(5, $result['version']);
        $result = $components->getConfigurationVersion($config->getComponentId(), $config->getConfigurationId(), 3);
        $this->assertArrayHasKey('name', $result);
        $this->assertEquals('Main', $result['name']);
        $result = $components->listConfigurationVersions($config);
        $this->assertCount(5, $result);

        $listOptions = new \Keboola\StorageApi\Options\Components\ListComponentsOptions();
        $listOptions->setInclude(array('rows'));
        $components = $components->listComponents($listOptions);

        $this->assertCount(1, $components);

        $component = reset($components);
        $configuration = reset($component['configurations']);

        $this->assertArrayHasKey('rows', $configuration);
        $this->assertCount(1, $configuration['rows']);

        $row = reset($configuration['rows']);
        $this->assertEquals(2, $row['version']);
        $this->assertEquals('main-1-1', $row['id']);
    }

    public function testComponentConfigsVersionsCreate()
    {
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components = new \Keboola\StorageApi\Components($this->_client);
        $newConfiguration = $components->addConfiguration($config);
        $this->assertEquals(1, $newConfiguration['version']);
        $this->assertEmpty($newConfiguration['state']);

        // version incremented to 2
        $newName = 'neco';
        $newDesc = 'some desc';
        $configurationData = array('x' => 'y');
        $config->setName($newName)
            ->setDescription($newDesc)
            ->setConfiguration($configurationData);
        $components->updateConfiguration($config);

        // version incremented to 3
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $configurationRow->setRowId('main-1-1');
        $components->addConfigurationRow($configurationRow);

        // version incremented to 4
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $configurationRow->setRowId('main-1-2');
        $components->addConfigurationRow($configurationRow);

        // rollback to 2 with one row
        $result = $components->createConfigurationFromVersion($config->getComponentId(), $config->getConfigurationId(), 3, 'New');
        $this->assertArrayHasKey('id', $result);
        $configuration = $components->getConfiguration($config->getComponentId(), $result['id']);
        $this->assertArrayHasKey('name', $configuration);
        $this->assertEquals('New', $configuration['name']);
        $this->assertArrayHasKey('description', $configuration);
        $this->assertEquals($newDesc, $configuration['description']);
        $this->assertArrayHasKey('version', $configuration);
        $this->assertEquals(1, $configuration['version']);
        $this->assertArrayHasKey('configuration', $configuration);
        $this->assertEquals($configurationData, $configuration['configuration']);
        $this->assertArrayHasKey('rows', $configuration);
        $this->assertCount(1, $configuration['rows']);
        $this->assertEquals('main-1-1', $configuration['rows'][0]['id']);

        // rollback to 1 with 0 rows
        $result = $components->createConfigurationFromVersion($config->getComponentId(), $config->getConfigurationId(), 1, 'New 2');
        $this->assertArrayHasKey('id', $result);
        $configuration = $components->getConfiguration($config->getComponentId(), $result['id']);
        $this->assertArrayHasKey('name', $configuration);
        $this->assertEquals('New 2', $configuration['name']);
        $this->assertArrayHasKey('description', $configuration);
        $this->assertEmpty($configuration['description']);
        $this->assertArrayHasKey('version', $configuration);
        $this->assertEquals(1, $configuration['version']);
        $this->assertArrayHasKey('configuration', $configuration);
        $this->assertEmpty($configuration['configuration']);
        $this->assertArrayHasKey('rows', $configuration);
        $this->assertCount(0, $configuration['rows']);
    }

    public function testListConfigs()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);

        $configs = $components->listComponents();
        $this->assertEmpty($configs);


        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main'));
        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-2')
            ->setConfiguration(array('x' => 'y'))
            ->setName('Main'));
        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('provisioning')
            ->setConfigurationId('main-1')
            ->setName('Main'));

        $configs = $components->listComponents();
        $this->assertCount(2, $configs);

        $configs = $components->listComponents((new \Keboola\StorageApi\Options\Components\ListComponentsOptions())
            ->setComponentType('writer'));

        $this->assertCount(2, $configs[0]['configurations']);
        $this->assertCount(1, $configs);

        $configuration = $configs[0]['configurations'][0];
        $this->assertArrayNotHasKey('configuration', $configuration);

        // list with configuration body
        $configs = $components->listComponents((new \Keboola\StorageApi\Options\Components\ListComponentsOptions())
            ->setComponentType('writer')
            ->setInclude(array('configuration')));

        $this->assertCount(2, $configs[0]['configurations']);
        $this->assertCount(1, $configs);

        $configuration = $configs[0]['configurations'][0];
        $this->assertArrayHasKey('configuration', $configuration);
    }

    public function testDuplicateConfigShouldNotBeCreated()
    {
        $options = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');

        $components = new \Keboola\StorageApi\Components($this->_client);
        $components->addConfiguration($options);

        try {
            $components->addConfiguration($options);
            $this->fail('Configuration should not be created');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('configurationAlreadyExists', $e->getStringCode());
        }
    }

    public function testPermissions()
    {
        $tokenId = $this->_client->createToken(array(), 'test');
        $token = $this->_client->getToken($tokenId);

        $client = new \Keboola\StorageApi\Client(array(
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ));

        $components = new \Keboola\StorageApi\Components($client);
        try {
            $components->listComponents();
            $this->fail('List components should not be allowed');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }
    }

    public function testTokenWithComponentAccess()
    {
        $this->_initEmptyTestBuckets();
        $accessibleComponents = array("provisioning");
        $tokenId = $this->_client->createToken(array($this->getTestBucketId(self::STAGE_IN) => "write"), 'test components', null, false, $accessibleComponents);
        $token = $this->_client->getToken($tokenId);

        $client = new \Keboola\StorageApi\Client(array(
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ));

        $components = new \Keboola\StorageApi\Components($client);
        $componentsList = $components->listComponents();
        $this->assertEmpty($componentsList);

        $config = $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('provisioning')
            ->setName('Main'));

        $componentsList = $components->listComponents();
        $this->assertCount(1, $componentsList);
        $this->assertEquals($config['id'], $componentsList[0]['configurations'][0]['id']);

        try {
            $config = $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
                ->setComponentId('wr-db')
                ->setName('Main'));
            $this->fail("Have not been granted permission to access this component, should throw exception");
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }
        $this->_client->dropToken($tokenId);
    }

    public function testTokenWithManageAllBucketsShouldHaveAccessToComponents()
    {
        $tokenId = $this->_client->createToken('manage', 'test components');
        $token = $this->_client->getToken($tokenId);
        $client = new \Keboola\StorageApi\Client(array(
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ));
        $components = new \Keboola\StorageApi\Components($client);
        $componentsList = $components->listComponents();
        $this->assertEmpty($componentsList);
        $config = $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setName('Main'));
        $componentsList = $components->listComponents();
        $this->assertCount(1, $componentsList);
        $this->assertEquals($config['id'], $componentsList[0]['configurations'][0]['id']);
        $this->_client->dropToken($tokenId);
    }

    public function testComponentConfigRowCreate()
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc');

        $components = new \Keboola\StorageApi\Components($this->_client);

        $components->addConfiguration($configuration);

        $component = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals('Main', $component['name']);
        $this->assertEquals('some desc', $component['description']);
        $this->assertEmpty($component['configuration']);
        $this->assertEquals(1, $component['version']);
        $this->assertInternalType('int', $component['version']);
        $this->assertInternalType('int', $component['creatorToken']['id']);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');

        $components->addConfigurationRow($configurationRow);

        $listOptions = new \Keboola\StorageApi\Options\Components\ListComponentsOptions();
        $listOptions->setInclude(array('rows'));
        $components = $components->listComponents($listOptions);

        $this->assertCount(1, $components);

        $component = reset($components);
        $this->assertEquals('wr-db', $component['id']);
        $this->assertCount(1, $component['configurations']);

        $configuration = reset($component['configurations']);
        $this->assertEquals('main-1', $configuration['id']);
        $this->assertEquals('Main', $configuration['name']);
        $this->assertEquals('some desc', $configuration['description']);

        $this->assertArrayHasKey('rows', $configuration);
        $this->assertCount(1, $configuration['rows']);

        $row = reset($configuration['rows']);
        $this->assertEquals('main-1-1', $row['id']);
        $this->assertEquals('', $row['name']);
        $this->assertEquals('', $row['description']);
        $this->assertEquals(false, $row['isDisabled']);

        $components = new \Keboola\StorageApi\Components($this->_client);

        $rows = $components->listConfigurationRows((new \Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions())
            ->setComponentId($component['id'])
            ->setConfigurationId($configuration['id']));

        $row = reset($rows);
        $this->assertEquals('main-1-1', $row['id']);

        $configuration = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(2, $configuration['version']);

        $this->assertArrayHasKey('rows', $configuration);
        $this->assertCount(1, $configuration['rows']);

        $row = reset($configuration['rows']);
        $this->assertEquals('main-1-1', $row['id']);
    }

    public function testComponentConfigRowCreateName()
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('main');

        $components = new \Keboola\StorageApi\Components($this->_client);
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow
            ->setRowId('main-1-1')
            ->setName('row name')
        ;

        $components->addConfigurationRow($configurationRow);

        $rows = $components->listConfigurationRows((new \Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1'));

        $row = reset($rows);
        $this->assertEquals('main-1-1', $row['id']);
        $this->assertEquals('row name', $row['name']);
    }


    public function testComponentConfigRowCreateDescription()
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('main');

        $components = new \Keboola\StorageApi\Components($this->_client);
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow
            ->setRowId('main-1-1')
            ->setDescription('row description')
        ;

        $components->addConfigurationRow($configurationRow);

        $rows = $components->listConfigurationRows((new \Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1'));

        $row = reset($rows);
        $this->assertEquals('main-1-1', $row['id']);
        $this->assertEquals('row description', $row['description']);
    }


    public function testComponentConfigRowCreateIsDisabled()
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('main');

        $components = new \Keboola\StorageApi\Components($this->_client);
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow
            ->setRowId('main-1-1')
            ->setIsDisabled(true)
        ;

        $components->addConfigurationRow($configurationRow);

        $rows = $components->listConfigurationRows((new \Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1'));

        $row = reset($rows);
        $this->assertEquals('main-1-1', $row['id']);
        $this->assertEquals(true, $row['isDisabled']);
    }


    public function testComponentConfigRowUpdateName()
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('main');

        $components = new \Keboola\StorageApi\Components($this->_client);
        $components->addConfiguration($configuration);

        $rowConfigurationData = [
            'some' => 'configuration'
        ];
        $rowDescription = 'some description';
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow
            ->setRowId('main-1-1')
            ->setName('row name')
            ->setConfiguration($rowConfigurationData)
            ->setDescription($rowDescription);
        ;

        $components->addConfigurationRow($configurationRow);

        $updateConfigurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $updateConfigurationRow
            ->setRowId('main-1-1')
            ->setName('altered row name')
        ;
        $components->updateConfigurationRow($updateConfigurationRow);

        $rows = $components->listConfigurationRows((new \Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1'));

        $row = reset($rows);
        $this->assertEquals('altered row name', $row['name']);
        $this->assertEquals($rowConfigurationData, $row['configuration']);
        $this->assertEquals($rowDescription, $row['description']);

        $configuration = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(3, $configuration['version']);
    }


    public function testComponentConfigRowUpdateDescription()
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('main');

        $components = new \Keboola\StorageApi\Components($this->_client);
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow
            ->setRowId('main-1-1')
            ->setDescription('row description')
        ;

        $components->addConfigurationRow($configurationRow);

        $updateConfigurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $updateConfigurationRow
            ->setRowId('main-1-1')
            ->setDescription('altered row description')
        ;
        $components->updateConfigurationRow($updateConfigurationRow);

        $rows = $components->listConfigurationRows((new \Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1'));

        $row = reset($rows);
        $this->assertEquals('altered row description', $row['description']);

        $configuration = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(3, $configuration['version']);
    }


    public function testComponentConfigRowUpdateIsDisabled()
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('main');

        $components = new \Keboola\StorageApi\Components($this->_client);
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow
            ->setRowId('main-1-1')
        ;

        $components->addConfigurationRow($configurationRow);

        $rows = $components->listConfigurationRows((new \Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1'));

        $row = reset($rows);
        $this->assertEquals(false, $row['isDisabled']);


        $updateConfigurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $updateConfigurationRow
            ->setRowId('main-1-1')
            ->setIsDisabled(true)
        ;
        $components->updateConfigurationRow($updateConfigurationRow);

        $rows = $components->listConfigurationRows((new \Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1'));

        $row = reset($rows);
        $this->assertEquals(true, $row['isDisabled']);

        $configuration = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(3, $configuration['version']);
    }


    public function testComponentConfigRowUpdateConfigEmpty()
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('main')
        ;

        $components = new \Keboola\StorageApi\Components($this->_client);
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow
            ->setRowId('main-1-1')
            ->setConfiguration(['foo' => 'bar'])
        ;
        $components->addConfigurationRow($configurationRow);

        $rows = $components->listConfigurationRows((new \Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1'));

        $row = reset($rows);
        $this->assertEquals(['foo' => 'bar'], $row['configuration']);

        $updateConfigurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $updateConfigurationRow
            ->setRowId('main-1-1')
            ->setConfiguration([])
        ;
        $components->updateConfigurationRow($updateConfigurationRow);

        $rows = $components->listConfigurationRows((new \Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1'));

        $row = reset($rows);
        $this->assertEquals([], $row['configuration']);

        $configuration = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(3, $configuration['version']);
    }

    public function testComponentConfigRowUpdateConfigEmptyWithEmpty()
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('main')
        ;

        $components = new \Keboola\StorageApi\Components($this->_client);
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow
            ->setRowId('main-1-1');
        ;
        $components->addConfigurationRow($configurationRow);

        $rows = $components->listConfigurationRows((new \Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1'));

        $row = reset($rows);
        $this->assertEmpty($row['configuration']);
        $this->assertEquals(1, $row['version']);

        $updateConfigurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $updateConfigurationRow
            ->setRowId('main-1-1')
            ->setConfiguration([])
        ;
        $components->updateConfigurationRow($updateConfigurationRow);

        $rows = $components->listConfigurationRows((new \Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1'));

        $row = reset($rows);
        $this->assertEmpty($row['configuration']);
        $this->assertEquals(1, $row['version']);
    }

    public function testComponentConfigRowUpdate()
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc');

        $originalToken = $this->_client->verifyToken();

        $components = new \Keboola\StorageApi\Components($this->_client);

        $components->addConfiguration($configuration);

        $component = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals('Main', $component['name']);
        $this->assertEquals('some desc', $component['description']);
        $this->assertEmpty($component['configuration']);
        $this->assertEquals(1, $component['version']);
        $this->assertInternalType('int', $component['version']);
        $this->assertInternalType('int', $component['creatorToken']['id']);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');

        $components->addConfigurationRow($configurationRow);

        $listOptions = new \Keboola\StorageApi\Options\Components\ListComponentsOptions();
        $listOptions->setInclude(array('rows'));
        $components = $components->listComponents($listOptions);

        $this->assertCount(1, $components);

        $component = reset($components);
        $this->assertEquals('wr-db', $component['id']);
        $this->assertCount(1, $component['configurations']);

        $configuration = reset($component['configurations']);
        $this->assertEquals('main-1', $configuration['id']);
        $this->assertEquals('Main', $configuration['name']);
        $this->assertEquals('some desc', $configuration['description']);

        $this->assertArrayHasKey('rows', $configuration);
        $this->assertCount(1, $configuration['rows']);

        $row = reset($configuration['rows']);
        $this->assertEquals('main-1-1', $row['id']);
        $this->assertEquals($originalToken['id'], $row['creatorToken']['id']);
        $this->assertEquals($originalToken['description'], $row['creatorToken']['description']);

        $components = new \Keboola\StorageApi\Components($this->_client);

        $rows = $components->listConfigurationRows((new \Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions())
            ->setComponentId($component['id'])
            ->setConfigurationId($configuration['id']));

        $originalRow = reset($rows);
        $this->assertEquals('main-1-1', $originalRow['id']);

        $component = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(2, $component['version']);

        $row = $components->updateConfigurationRow($configurationRow);

        $this->assertEquals(1, $row['version']);
        $this->assertEmpty($row['configuration']);

        $configurationData = array('test' => 1);
        $configurationChangeDescription = 'Change description test';

        $configurationRow->setConfiguration($configurationData)
            ->setChangeDescription($configurationChangeDescription);

        $newTokenId = $this->_client->createToken([], 'tests', 60, false, ['wr-db']);
        $newToken = $this->_client->getToken($newTokenId);
        $newClient = new \Keboola\StorageApi\Client([
            'token' => $newToken['token'],
            'url' => STORAGE_API_URL,
        ]);

        $newComponents = new \Keboola\StorageApi\Components($newClient);
        $row = $newComponents->updateConfigurationRow($configurationRow);

        $this->assertEquals(2, $row['version']);
        $this->assertEquals($configurationData, $row['configuration']);
        $this->assertEquals($originalRow['created'], $row['created'], 'row created data should not be changed');

        $version = $components->getConfigurationRowVersion(
            $configurationRow->getComponentConfiguration()->getComponentId(),
            $configurationRow->getComponentConfiguration()->getConfigurationId(),
            $configurationRow->getRowId(),
            2
        );

        $this->assertArrayHasKey('changeDescription', $version);
        $this->assertEquals($configurationChangeDescription, $version['changeDescription']);
        $this->assertNotEmpty($version['created']);
        $this->assertEquals($newToken['id'], $version['creatorToken']['id']);
        $this->assertEquals($newToken['description'], $version['creatorToken']['description']);
    }

    public function testComponentConfigRowDelete()
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc');

        $components = new \Keboola\StorageApi\Components($this->_client);

        $components->addConfiguration($configuration);

        $component = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals('Main', $component['name']);
        $this->assertEquals('some desc', $component['description']);
        $this->assertEmpty($component['configuration']);
        $this->assertEquals(1, $component['version']);
        $this->assertInternalType('int', $component['version']);
        $this->assertInternalType('int', $component['creatorToken']['id']);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');

        $components->addConfigurationRow($configurationRow);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-2');

        $components->addConfigurationRow($configurationRow);

        $listOptions = new \Keboola\StorageApi\Options\Components\ListComponentsOptions();
        $listOptions->setInclude(array('rows'));
        $components = $components->listComponents($listOptions);

        $this->assertCount(1, $components);

        $component = reset($components);
        $this->assertEquals('wr-db', $component['id']);
        $this->assertCount(1, $component['configurations']);

        $configuration = reset($component['configurations']);
        $this->assertEquals('main-1', $configuration['id']);
        $this->assertEquals('Main', $configuration['name']);
        $this->assertEquals('some desc', $configuration['description']);

        $this->assertArrayHasKey('rows', $configuration);
        $this->assertCount(2, $configuration['rows']);

        $row = reset($configuration['rows']);
        $this->assertEquals('main-1-1', $row['id']);

        $components = new \Keboola\StorageApi\Components($this->_client);

        $rows = $components->listConfigurationRows((new \Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions())
            ->setComponentId($component['id'])
            ->setConfigurationId($configuration['id']));

        $this->assertCount(2, $rows);

        $row = reset($rows);
        $this->assertEquals('main-1-1', $row['id']);

        $component = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(3, $component['version']);

        $components->deleteConfigurationRow(
            $configurationRow->getComponentConfiguration()->getComponentId(),
            $configurationRow->getComponentConfiguration()->getConfigurationId(),
            $configurationRow->getRowId()
        );

        $components = new \Keboola\StorageApi\Components($this->_client);

        $rows = $components->listConfigurationRows((new \Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions())
            ->setComponentId($configurationRow->getComponentConfiguration()->getComponentId())
            ->setConfigurationId($configurationRow->getComponentConfiguration()->getConfigurationId()));

        $this->assertCount(1, $rows);

        $row = reset($rows);
        $this->assertEquals('main-1-1', $row['id']);

        $component = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(4, $component['version']);
    }

    public function testComponentConfigDeletedRowId()
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('transformation')
            ->setConfigurationId('main')
            ->setName("Main");
        $components = new \Keboola\StorageApi\Components($this->_client);
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow
            ->setRowId("test")
            ->setConfiguration(["key" => "value"]);
        $components->addConfigurationRow($configurationRow);
        $components->deleteConfigurationRow("transformation", "main", "test");
        $components->addConfigurationRow($configurationRow->setConfiguration(["key" => "newValue"]));

        $listRowsOptions = new \Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions();
        $listRowsOptions
            ->setComponentId("transformation")
            ->setConfigurationId("main");
        $rows = $components->listConfigurationRows($listRowsOptions);
        $this->assertCount(1, $rows);

        $row = reset($rows);
        $this->assertEquals(2, $row['version']);
        $this->assertEquals(["key" => "newValue"], $row["configuration"]);
    }

    public function testComponentConfigRowVersionsList()
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc');

        $components = new \Keboola\StorageApi\Components($this->_client);

        $components->addConfiguration($configuration);

        $component = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals('Main', $component['name']);
        $this->assertEquals('some desc', $component['description']);
        $this->assertEmpty($component['configuration']);
        $this->assertEquals(1, $component['version']);
        $this->assertInternalType('int', $component['version']);
        $this->assertInternalType('int', $component['creatorToken']['id']);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');

        $components->addConfigurationRow($configurationRow);

        $listOptions = new \Keboola\StorageApi\Options\Components\ListComponentsOptions();
        $listOptions->setInclude(array('rows'));
        $components = $components->listComponents($listOptions);

        $this->assertCount(1, $components);

        $component = reset($components);
        $this->assertEquals('wr-db', $component['id']);
        $this->assertCount(1, $component['configurations']);

        $configuration = reset($component['configurations']);
        $this->assertEquals('main-1', $configuration['id']);
        $this->assertEquals('Main', $configuration['name']);
        $this->assertEquals('some desc', $configuration['description']);

        $this->assertArrayHasKey('rows', $configuration);
        $this->assertCount(1, $configuration['rows']);

        $row = reset($configuration['rows']);
        $this->assertEquals('main-1-1', $row['id']);
        $this->assertEquals('', $row['name']);
        $this->assertEquals('', $row['description']);
        $this->assertEquals(false, $row['isDisabled']);

        $components = new \Keboola\StorageApi\Components($this->_client);

        $rows = $components->listConfigurationRows((new \Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions())
            ->setComponentId($component['id'])
            ->setConfigurationId($configuration['id']));

        $row = reset($rows);
        $this->assertEquals('main-1-1', $row['id']);

        $component = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(2, $component['version']);

        $row = $components->updateConfigurationRow($configurationRow);

        $this->assertEquals(1, $row['version']);
        $this->assertEmpty($row['configuration']);

        $configurationData = array('test' => 1);

        $configurationRow->setConfiguration($configurationData);

        $row = $components->updateConfigurationRow($configurationRow);

        $this->assertEquals(2, $row['version']);
        $this->assertEquals($configurationData, $row['configuration']);


        $versions = $components->listConfigurationRowVersions(
            (new \Keboola\StorageApi\Options\Components\ListConfigurationRowVersionsOptions())
                ->setComponentId('wr-db')
                ->setConfigurationId('main-1')
                ->setRowId($configurationRow->getRowId())
        );

        $this->assertCount(2, $versions);

        foreach ($versions as $version) {
            $this->assertArrayHasKey('version', $version);
            $this->assertArrayHasKey('created', $version);
            $this->assertArrayHasKey('creatorToken', $version);
        }

        $versions = $components->listConfigurationRowVersions(
            (new \Keboola\StorageApi\Options\Components\ListConfigurationRowVersionsOptions())
                ->setComponentId('wr-db')
                ->setConfigurationId('main-1')
                ->setRowId($configurationRow->getRowId())
                ->setInclude(array('configuration'))
        );

        $this->assertCount(2, $versions);

        foreach ($versions as $version) {
            $this->assertArrayHasKey('version', $version);
            $this->assertArrayHasKey('created', $version);
            $this->assertArrayHasKey('creatorToken', $version);
            $this->assertArrayHasKey('configuration', $version);

            $rowVersion = $components->getConfigurationRowVersion(
                'wr-db',
                'main-1',
                $configurationRow->getRowId(),
                $version['version']
            );

            $this->assertArrayHasKey('version', $rowVersion);
            $this->assertArrayHasKey('created', $rowVersion);
            $this->assertArrayHasKey('creatorToken', $rowVersion);
            $this->assertArrayHasKey('configuration', $rowVersion);
        }

        $versions = $components->listConfigurationRowVersions(
            (new \Keboola\StorageApi\Options\Components\ListConfigurationRowVersionsOptions())
                ->setComponentId('wr-db')
                ->setConfigurationId('main-1')
                ->setRowId($configurationRow->getRowId())
                ->setLimit(1)
                ->setOffset(1)
        );

        $this->assertCount(1, $versions);

        foreach ($versions as $version) {
            $this->assertArrayHasKey('version', $version);
            $this->assertArrayHasKey('created', $version);
            $this->assertArrayHasKey('creatorToken', $version);
        }
    }

    public function testComponentConfigRowVersionRollback()
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc');

        $components = new \Keboola\StorageApi\Components($this->_client);

        $components->addConfiguration($configuration);

        $component = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals('Main', $component['name']);
        $this->assertEquals('some desc', $component['description']);
        $this->assertEmpty($component['configuration']);
        $this->assertEquals(1, $component['version']);
        $this->assertInternalType('int', $component['version']);
        $this->assertInternalType('int', $component['creatorToken']['id']);

        $rowConfiguration = array('my-value' => 666);

        // add first row
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');
        $configurationRow->setConfiguration($rowConfiguration);

        $components->addConfigurationRow($configurationRow);

        $listOptions = new \Keboola\StorageApi\Options\Components\ListComponentsOptions();
        $listOptions->setInclude(array('rows'));
        $components = $components->listComponents($listOptions);

        $this->assertCount(1, $components);

        $component = reset($components);
        $this->assertEquals('wr-db', $component['id']);
        $this->assertCount(1, $component['configurations']);

        $configuration = reset($component['configurations']);
        $this->assertEquals('main-1', $configuration['id']);
        $this->assertEquals('Main', $configuration['name']);
        $this->assertEquals('some desc', $configuration['description']);

        $this->assertArrayHasKey('rows', $configuration);
        $this->assertCount(1, $configuration['rows']);

        $originalRow = reset($configuration['rows']);
        $this->assertEquals('main-1-1', $originalRow['id']);

        $components = new \Keboola\StorageApi\Components($this->_client);

        $rows = $components->listConfigurationRows((new \Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions())
            ->setComponentId($component['id'])
            ->setConfigurationId($configuration['id']));

        $row = reset($rows);
        $this->assertEquals('main-1-1', $row['id']);

        $component = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(2, $component['version']);

        // update row 1st - without change
        $rowVersion1 = $components->updateConfigurationRow($configurationRow);

        $this->assertEquals(1, $rowVersion1['version']);
        $this->assertEquals('', $rowVersion1['name']);
        $this->assertEquals('', $rowVersion1['description']);
        $this->assertEquals(false, $rowVersion1['isDisabled']);


        $configurationData = array('test' => 1);

        $configurationRow->setConfiguration($configurationData)
            ->setChangeDescription('some change');

        // update row 2nd
        $rowVersion2 = $components->updateConfigurationRow($configurationRow);

        $this->assertEquals(2, $rowVersion2['version']);
        $this->assertEquals($configurationData, $rowVersion2['configuration']);
        $this->assertEquals('some change', $rowVersion2['changeDescription']);
        $this->assertEquals('', $rowVersion2['name']);
        $this->assertEquals('', $rowVersion2['description']);
        $this->assertEquals(false, $rowVersion2['isDisabled']);


        // update row 3rd
        $configurationData = array('test' => 2);
        $configurationRow->setConfiguration($configurationData)
            ->setChangeDescription(null);

        $rowVersion3 = $components->updateConfigurationRow($configurationRow);

        $this->assertEquals(3, $rowVersion3['version']);
        $this->assertEquals($configurationData, $rowVersion3['configuration']);
        $this->assertNull($rowVersion3['changeDescription']);

        // rollback to version 2
        $rowVersion4 = $components->rollbackConfigurationRow(
            'wr-db',
            'main-1',
            $configurationRow->getRowId(),
            2
        );

        $this->assertEquals(4, $rowVersion4['version'], 'Rollback creates new version of the row');
        $this->assertEquals('Rollback from version 2', $rowVersion4['changeDescription'], 'Rollback creates automatic description');
        $this->assertArrayEqualsExceptKeys($rowVersion2, $rowVersion4, ['version', 'changeDescription']);

        // rollback to version 3
        $rowVersion5 = $components->rollbackConfigurationRow(
            'wr-db',
            'main-1',
            $configurationRow->getRowId(),
            3,
            'Custom rollback message'
        );

        $this->assertEquals(5, $rowVersion5['version'], 'Rollback creates new version of the row');
        $this->assertEquals('Custom rollback message', $rowVersion5['changeDescription']);
        $this->assertArrayEqualsExceptKeys($rowVersion3, $rowVersion5, ['version', 'changeDescription']);

        $versions = $components->listConfigurationRowVersions(
            (new \Keboola\StorageApi\Options\Components\ListConfigurationRowVersionsOptions())
                ->setComponentId('wr-db')
                ->setConfigurationId('main-1')
                ->setRowId($configurationRow->getRowId())
        );

        $this->assertCount(5, $versions);
    }

    public function testComponentConfigRowVersionCreate()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);

        $configurationData = array('my-value' => 666);

        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc');

        $components->addConfiguration($configuration);

        $configuration2 = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration2
            ->setComponentId($configuration->getComponentId())
            ->setConfigurationId('main-2')
            ->setName('Main')
            ->setDescription('some desc');

        $components->addConfiguration($configuration2);


        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');
        $configurationRow->setConfiguration($configurationData);

        $components->addConfigurationRow($configurationRow);

        // copy to same first configuration
        $row = $components->createConfigurationRowFromVersion(
            $configuration->getComponentId(),
            $configuration->getConfigurationId(),
            'main-1-1',
            1
        );

        $this->assertArrayHasKey('id', $row);
        $this->assertArrayHasKey('version', $row);
        $this->assertArrayHasKey('configuration', $row);

        $this->assertEquals(1, $row['version']);
        $this->assertEquals($configurationData, $row['configuration']);
        $this->assertEquals('', $row['name']);
        $this->assertEquals('', $row['description']);
        $this->assertEquals(false, $row['isDisabled']);

        $rows = $components->listConfigurationRows((new \Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions())
            ->setComponentId($configuration->getComponentId())
            ->setConfigurationId($configuration->getConfigurationId()));

        $this->assertCount(2, $rows);

        // copy to same second configuration
        $row = $components->createConfigurationRowFromVersion(
            $configuration->getComponentId(),
            $configuration->getConfigurationId(),
            'main-1-1',
            1,
            $configuration2->getConfigurationId()
        );

        $rows = $components->listConfigurationRows((new \Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions())
            ->setComponentId($configuration->getComponentId())
            ->setConfigurationId($configuration->getConfigurationId()));

        $this->assertCount(2, $rows);

        $this->assertArrayHasKey('id', $row);
        $this->assertArrayHasKey('version', $row);
        $this->assertArrayHasKey('configuration', $row);
        $this->assertArrayHasKey('isDisabled', $row);
        $this->assertArrayHasKey('name', $row);
        $this->assertArrayHasKey('description', $row);


        $this->assertEquals(1, $row['version']);
        $this->assertEquals($configurationData, $row['configuration']);

        $rows = $components->listConfigurationRows((new \Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions())
            ->setComponentId($configuration2->getComponentId())
            ->setConfigurationId($configuration2->getConfigurationId()));

        $this->assertCount(1, $rows);
    }

    public function testGetComponentConfigurations()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);

        $configs = $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId('transformation')
        );
        $this->assertEmpty($configs);

        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('transformation')
            ->setConfigurationId('main-1')
            ->setName('Main 1'));
        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('transformation')
            ->setConfigurationId('main-2')
            ->setName('Main 2'));

        $configs = $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId('transformation')
        );
        $this->assertCount(2, $configs);
    }

    public function testGetComponentConfigurationsWithConfigAndRows()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);

        $configs = $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId('transformation')
        );
        $this->assertEmpty($configs);

        $configData1 = ['key1' => 'val1'];
        $configData2 = ['key2' => 'val2'];

        $configuration = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('transformation')
            ->setConfigurationId('main-1')
            ->setName('Main 1')
            ->setConfiguration($configData1);

        $components->addConfiguration($configuration);
        $components->addConfigurationRow((new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration))
            ->setRowId('row1')
            ->setConfiguration($configData2));
        $configs = $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId('transformation')
        );
        $this->assertCount(1, $configs);
        $this->assertEquals($configData1, $configs[0]['configuration']);
        $this->assertEquals($configData2, $configs[0]['rows'][0]['configuration']);
    }


    /**
     * Create configuration with few rows, update some row and then rollback to configuration with updated row
     */
    public function testChangeDescription()
    {
        // test 1: create config
        $createChangeDescription = 'Create configuration';
        $componentConfig = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setConfiguration(['a' => 'b'])
            ->setName('Main')
            ->setChangeDescription($createChangeDescription);
        $components = new \Keboola\StorageApi\Components($this->_client);
        $newConfiguration = $components->addConfiguration($componentConfig);
        $this->assertEquals($createChangeDescription, $newConfiguration['changeDescription']);
        $config = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals($createChangeDescription, $config['changeDescription']);

        // test 2: update config
        $updateChangeDescription = 'Update configuration';
        $config = (new \Keboola\StorageApi\Options\Components\Configuration());
        $config
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('test')
            ->setChangeDescription($updateChangeDescription);
        $updatedConfiguration = $components->updateConfiguration($config);
        $this->assertEquals($updateChangeDescription, $updatedConfiguration['changeDescription']);
        $config = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals($updateChangeDescription, $config['changeDescription']);

        // test 3: create row
        $addRowChangeDescription = 'Added row #1';
        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($componentConfig);
        $firstRowConfig = array('first' => 1);
        $rowConfig
            ->setConfiguration($firstRowConfig)
            ->setChangeDescription($addRowChangeDescription);
        $createdRow = $components->addConfigurationRow($rowConfig);
        $this->assertEquals($addRowChangeDescription, $createdRow['changeDescription']);
        $config = $components->getConfiguration('wr-db', $componentConfig->getConfigurationId());
        $this->assertEquals($addRowChangeDescription, $config['changeDescription']);
        $this->assertEquals($addRowChangeDescription, $config['rows'][0]['changeDescription']);

        // test 4: update row
        $updateRowChangeDescription = 'Modified row #1';
        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($componentConfig);
        $firstRowUpdatedConfig = array('first' => 22);
        $rowConfig
            ->setConfiguration($firstRowUpdatedConfig)
            ->setRowId($createdRow['id'])
            ->setChangeDescription($updateRowChangeDescription);
        $updatedRow = $components->updateConfigurationRow($rowConfig);
        $this->assertEquals($updateRowChangeDescription, $updatedRow['changeDescription']);
        $config = $components->getConfiguration('wr-db', $newConfiguration['id']);
        $this->assertEquals($updateRowChangeDescription, $config['changeDescription']);
        $this->assertEquals($updateRowChangeDescription, $config['rows'][0]['changeDescription']);

        // test 5: rollback config
        $rollbackChangeDescription = 'Rollback from version #3';
        $components->rollbackConfiguration('wr-db', $componentConfig->getConfigurationId(), 3, $rollbackChangeDescription);
        $config = $components->getConfiguration('wr-db', $componentConfig->getConfigurationId());
        $this->assertEquals($rollbackChangeDescription, $config['changeDescription']);

        // test 6: copy config
        $copyChangeDescription = 'Copy from ABC';
        $copyConfig = $components->createConfigurationFromVersion('wr-db', $componentConfig->getConfigurationId(), 5, 'New 2', null, $copyChangeDescription);
        $config = $components->getConfiguration('wr-db', $copyConfig['id']);
        $this->assertEquals($copyChangeDescription, $config['changeDescription']);

        // test 7: rollback row
        $rollbackRowChangeDescription = 'Rollback some other version';
        $rollbackRow = $components->rollbackConfigurationRow('wr-db', $componentConfig->getConfigurationId(), $createdRow['id'], 3, $rollbackRowChangeDescription);
        $this->assertEquals($rollbackRowChangeDescription, $rollbackRow['changeDescription']);
        $config = $components->getConfiguration('wr-db', $newConfiguration['id']);
        $this->assertEquals($rollbackRowChangeDescription, $config['changeDescription']);
        $this->assertEquals($rollbackRowChangeDescription, $config['rows'][0]['changeDescription']);

        // test 8: copy row
        $copyRowChangeDescription = 'Copy a row to a config';
        $copyRow = $components->createConfigurationRowFromVersion('wr-db', $componentConfig->getConfigurationId(), $createdRow['id'], 4, $copyConfig['id'], $copyRowChangeDescription);
        $this->assertEquals($copyRowChangeDescription, $copyRow['changeDescription']);
        $config = $components->getConfiguration('wr-db', $copyConfig['id']);
        $this->assertEquals($copyRowChangeDescription, $config['changeDescription']);
        $this->assertEquals($copyRowChangeDescription, $config['rows'][1]['changeDescription']);

        // test 9: delete row
        $deleteRowChangeDescription = 'Delete a row, just like that!!!';
        $components->deleteConfigurationRow('wr-db', $componentConfig->getConfigurationId(), $createdRow['id'], $deleteRowChangeDescription);
        $config = $components->getConfiguration('wr-db', $componentConfig->getConfigurationId());
        $this->assertEquals($deleteRowChangeDescription, $config['changeDescription']);
    }
    
    public function testConfigurationNameAndDescriptionShouldNotBeTrimmed()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $config = $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName("name\n")
            ->setDescription("description\n"));

        $this->assertEquals("name\n", $config["name"]);
        $this->assertEquals("description\n", $config["description"]);

        $config = $components->updateConfiguration((new  \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName("name2\n")
            ->setDescription("description2\n"));

        $this->assertEquals("name2\n", $config["name"]);
        $this->assertEquals("description2\n", $config["description"]);
    }

    public function testConfigurationRowNameAndDescriptionShouldNotBeTrimmed()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName("name")
            ->setDescription("description");
        $components->addConfiguration($config);

        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $rowConfig->setName("name\n");
        $rowConfig->setDescription("description\n");
        $createdRow = $components->addConfigurationRow($rowConfig);
        $this->assertEquals("name\n", $createdRow["name"]);
        $this->assertEquals("description\n", $createdRow["description"]);

        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $rowConfig->setRowId($createdRow["id"]);
        $rowConfig->setName("name2\n");
        $rowConfig->setDescription("description2\n");

        $updatedRow = $components->updateConfigurationRow($rowConfig);
        $this->assertEquals("name2\n", $updatedRow["name"]);
        $this->assertEquals("description2\n", $updatedRow["description"]);
    }

    /**
     * tests for https://github.com/keboola/connection/issues/977
     */
    public function testRowChangesAfterConfigurationRollback()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);

        // config version 1
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName("name")
            ->setDescription("description");
        $components->addConfiguration($config);

        // config version 2
        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $createdRow = $components->addConfigurationRow($rowConfig);

        // config version 3
        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $rowConfig->setRowId($createdRow["id"]);
        $rowConfig->setName("name");
        $rowConfig->setDescription("description");
        $rowConfig->setIsDisabled(true);
        $components->updateConfigurationRow($rowConfig);

        // rollback config version 2
        $components->rollbackConfiguration('wr-db', $config->getConfigurationId(), 2);
        $response = $components->getConfiguration('wr-db', $config->getConfigurationId());
        $this->assertEquals("", $response["rows"][0]["name"]);
        $this->assertEquals("", $response["rows"][0]["description"]);
        $this->assertEquals(false, $response["rows"][0]["isDisabled"]);

        // rollback config version 3
        $components->rollbackConfiguration('wr-db', $config->getConfigurationId(), 3);
        $response = $components->getConfiguration('wr-db', $config->getConfigurationId());
        $this->assertEquals("name", $response["rows"][0]["name"]);
        $this->assertEquals("description", $response["rows"][0]["description"]);
        $this->assertEquals(true, $response["rows"][0]["isDisabled"]);
    }

    /**
     * tests for https://github.com/keboola/connection/issues/977
     */
    public function testRowChangesAfterConfigurationCopy()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);

        // config version 1
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName("name")
            ->setDescription("description");
        $components->addConfiguration($config);

        // config version 2
        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $createdRow = $components->addConfigurationRow($rowConfig);

        // config version 3
        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $rowConfig->setRowId($createdRow["id"]);
        $rowConfig->setName("name");
        $rowConfig->setDescription("description");
        $rowConfig->setIsDisabled(true);
        $components->updateConfigurationRow($rowConfig);

        // copy config version 2
        $copiedConfig = $components->createConfigurationFromVersion('wr-db', $config->getConfigurationId(), 2, 'test');
        $response = $components->getConfiguration('wr-db', $copiedConfig["id"]);
        $this->assertSame('Copied from configuration "main-1" version 2', $response['changeDescription']);
        $this->assertEquals("", $response["rows"][0]["name"]);
        $this->assertEquals("", $response["rows"][0]["description"]);
        $this->assertEquals('Copied from configuration "main-1" version 2', $response["rows"][0]["changeDescription"]);
        $this->assertEquals(false, $response["rows"][0]["isDisabled"]);

        // copy config version 3
        $copiedConfig = $components->createConfigurationFromVersion('wr-db', $config->getConfigurationId(), 3, 'test');
        $response = $components->getConfiguration('wr-db', $copiedConfig["id"]);
        $this->assertSame('Copied from configuration "main-1" version 3', $response['changeDescription']);
        $this->assertEquals("name", $response["rows"][0]["name"]);
        $this->assertEquals("description", $response["rows"][0]["description"]);
        $this->assertEquals('Copied from configuration "main-1" version 3', $response["rows"][0]["changeDescription"]);
        $this->assertEquals(true, $response["rows"][0]["isDisabled"]);
    }

    /**
     * tests for https://github.com/keboola/connection/issues/977
     */
    public function testRowChangesAfterRowRollback()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);

        // config version 1
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName("name")
            ->setDescription("description");
        $components->addConfiguration($config);

        // config version 2, row version 1
        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $createdRow = $components->addConfigurationRow($rowConfig);

        // config version 3, row version 2
        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $rowConfig->setRowId($createdRow["id"]);
        $rowConfig->setName("name");
        $rowConfig->setDescription("description");
        $rowConfig->setIsDisabled(true);
        $components->updateConfigurationRow($rowConfig);

        // rollback row version 1
        $components->rollbackConfigurationRow('wr-db', $config->getConfigurationId(), $createdRow["id"], 1);
        $response = $components->getConfiguration('wr-db', $config->getConfigurationId());
        $this->assertEquals("", $response["rows"][0]["name"]);
        $this->assertEquals("", $response["rows"][0]["description"]);
        $this->assertEquals(false, $response["rows"][0]["isDisabled"]);

        // rollback row version 2
        $components->rollbackConfigurationRow('wr-db', $config->getConfigurationId(), $createdRow["id"], 2);
        $response = $components->getConfiguration('wr-db', $config->getConfigurationId());
        $this->assertEquals("name", $response["rows"][0]["name"]);
        $this->assertEquals("description", $response["rows"][0]["description"]);
        $this->assertEquals(true, $response["rows"][0]["isDisabled"]);
    }

    /**
     * tests for https://github.com/keboola/connection/issues/977
     */
    public function testRowChangesAfterRowCopy()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);

        // config version 1
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName("name")
            ->setDescription("description");
        $components->addConfiguration($config);

        // config version 2, row version 1
        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $createdRow = $components->addConfigurationRow($rowConfig);

        // config version 3, row version 2
        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $rowConfig->setRowId($createdRow["id"]);
        $rowConfig->setName("name");
        $rowConfig->setDescription("description");
        $rowConfig->setIsDisabled(true);
        $components->updateConfigurationRow($rowConfig);

        // copy row version 1
        $createdRow2 = $components->createConfigurationRowFromVersion('wr-db', $config->getConfigurationId(), $createdRow["id"], 1);
        $response = $components->getConfiguration('wr-db', $config->getConfigurationId());
        $this->assertStringMatchesFormat('Row %d copied from configuration "main-1" row %d version 1', $response['changeDescription']);
        $this->assertEquals($createdRow["id"], $response["rows"][0]["id"]);
        $this->assertEquals("name", $response["rows"][0]["name"]);
        $this->assertEquals("description", $response["rows"][0]["description"]);
        $this->assertEquals("", $response["rows"][0]["changeDescription"]);
        $this->assertEquals(true, $response["rows"][0]["isDisabled"]);
        $this->assertEquals($createdRow2["id"], $response["rows"][1]["id"]);
        $this->assertEquals("", $response["rows"][1]["name"]);
        $this->assertEquals("", $response["rows"][1]["description"]);
        $this->assertStringMatchesFormat('Copied from configuration "main-1" row %d version 1', $response["rows"][1]["changeDescription"]);
        $this->assertEquals(false, $response["rows"][1]["isDisabled"]);

        // copy row version 2
        $createdRow3 = $components->createConfigurationRowFromVersion('wr-db', $config->getConfigurationId(), $createdRow["id"], 2);
        $response = $components->getConfiguration('wr-db', $config->getConfigurationId());
        $this->assertStringMatchesFormat('Row %d copied from configuration "main-1" row %d version 2', $response['changeDescription']);
        $this->assertEquals($createdRow["id"], $response["rows"][0]["id"]);
        $this->assertEquals("name", $response["rows"][0]["name"]);
        $this->assertEquals("description", $response["rows"][0]["description"]);
        $this->assertEquals("", $response["rows"][0]["changeDescription"]);
        $this->assertEquals(true, $response["rows"][0]["isDisabled"]);
        $this->assertEquals($createdRow2["id"], $response["rows"][1]["id"]);
        $this->assertEquals("", $response["rows"][1]["name"]);
        $this->assertEquals("", $response["rows"][1]["description"]);
        $this->assertStringMatchesFormat('Copied from configuration "main-1" row %d version 1', $response["rows"][1]["changeDescription"]);
        $this->assertEquals(false, $response["rows"][1]["isDisabled"]);
        $this->assertEquals($createdRow3["id"], $response["rows"][2]["id"]);
        $this->assertEquals("name", $response["rows"][2]["name"]);
        $this->assertEquals("description", $response["rows"][2]["description"]);
        $this->assertStringMatchesFormat('Copied from configuration "main-1" row %d version 2', $response["rows"][2]["changeDescription"]);
        $this->assertEquals(true, $response["rows"][2]["isDisabled"]);
    }

    public function testStateAttributeNotPresentInVersions()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $this->assertArrayNotHasKey('state', $components->getConfigurationVersion('wr-db', 'main-1', 1));
    }

    public function testRollbackPreservesState()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $state = ['key' => 'val'];
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setState(['unknown' => 'undefined']);
        $components->addConfiguration($configuration);

        $configuration->setName("Updated name");
        $components->updateConfiguration($configuration);

        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setState($state);
        $components->updateConfiguration($configuration);

        $components->rollbackConfiguration('wr-db', 'main-1', 1);

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(3, $configurationResponse['version']);
        $this->assertEquals($state, $configurationResponse['state']);
    }

    public function testCopyPreservesState()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $state = ['key' => 'val'];
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setState(['unknown' => 'undefined']);
        $components->addConfiguration($configuration);

        $configuration->setName("Updated name");
        $components->updateConfiguration($configuration);

        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setState($state);
        $components->updateConfiguration($configuration);


        $newConfig = $components->createConfigurationFromVersion('wr-db', 'main-1', 1, 'main-2');

        $configurationResponse = $components->getConfiguration('wr-db', $newConfig['id']);
        $this->assertEquals($state, $configurationResponse['state']);
    }

    public function testRevertingConfigRowVersionWillNotCreateEmptyConfiguration()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $configuration
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main');
        $configurationRow = new ConfigurationRow($configuration);
        $configurationRow->setChangeDescription('Test description');

        $components->addConfiguration($configuration);
        $rowArray = $components->addConfigurationRow($configurationRow);
        $originalConfigurationArray = $components->getConfiguration($componentId, $configurationId);
        $components->deleteConfigurationRow($componentId, $configurationId, $rowArray['id']);
        $components->rollbackConfiguration($componentId, $configurationId, $originalConfigurationArray['version']);
        $rollbackedConfigurationArray = $components->getConfiguration($componentId, $configurationId);

        $originalConfigRow = $originalConfigurationArray['rows'][0];
        $rollbackedConfigRow = $rollbackedConfigurationArray['rows'][0];
        $this->assertArrayEqualsExceptKeys($originalConfigRow, $rollbackedConfigRow, ['version']);
    }
}
