<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */

use Keboola\Csv\CsvFile;

class Keboola_StorageApi_ComponentsTest extends StorageApiTestCase
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
	}

	public function testComponentConfigCreate()
	{
		$components = new \Keboola\StorageApi\Components($this->_client);
		$components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
			->setComponentId('gooddata-writer')
			->setConfigurationId('main-1')
			->setName('Main')
			->setDescription('some desc')
		);

		$component = $components->getConfiguration('gooddata-writer', 'main-1');
		$this->assertEquals('Main', $component['name']);
		$this->assertEquals('some desc', $component['description']);
		$this->assertEmpty($component['configuration']);
		$this->assertEquals(0, $component['version']);

		$components = $components->listComponents();
		$this->assertCount(1, $components);

		$component = reset($components);
		$this->assertEquals('gooddata-writer', $component['id']);
		$this->assertCount(1, $component['configurations']);

		$configuration = reset($component['configurations']);
		$this->assertEquals('main-1', $configuration['id']);
		$this->assertEquals('Main', $configuration['name']);
		$this->assertEquals('some desc', $configuration['description']);
	}

	public function testNonJsonConfigurationShouldNotBeAllowed()
	{
		try {
			$this->_client->apiPost('storage/components/gooddata-writer/configs', array(
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

	public function testComponentConfigCreateWithConfigurationJson()
	{
		$configuration = array(
			'queries' => array(
				array(
					'id' => 1,
					'query' => 'SELECT * from some_table',
				)
			),
		);
		$components = new \Keboola\StorageApi\Components($this->_client);
		$components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
				->setComponentId('gooddata-writer')
				->setConfigurationId('main-1')
				->setName('Main')
				->setDescription('some desc')
				->setConfiguration($configuration)
		);

		$config = $components->getConfiguration('gooddata-writer', 'main-1');

		$this->assertEquals($configuration, $config['configuration']);
		$this->assertEquals(0, $config['version']);
	}

	public function testComponentConfigCreateIdAutoCreate()
	{
		$components = new \Keboola\StorageApi\Components($this->_client);
		$component = $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
				->setComponentId('gooddata-writer')
				->setName('Main')
				->setDescription('some desc')
		);
		$this->assertNotEmpty($component['id']);
		$component = $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
				->setComponentId('gooddata-writer')
				->setName('Main')
				->setDescription('some desc')
		);
		$this->assertNotEmpty($component['id']);
	}

	public function testComponentConfigUpdate()
	{
		$config = (new \Keboola\StorageApi\Options\Components\Configuration())
			->setComponentId('gooddata-writer')
			->setConfigurationId('main-1')
			->setName('Main');
		$components = new \Keboola\StorageApi\Components($this->_client);
		$newConfiguration = $components->addConfiguration($config);
		$this->assertEquals(0, $newConfiguration['version']);


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
		$this->assertEquals(1, $configuration['version']);

		$config = (new \Keboola\StorageApi\Options\Components\Configuration())
			->setComponentId('gooddata-writer')
			->setConfigurationId('main-1')
			->setDescription('neco');

		$components->updateConfiguration($config);
		$configuration = $components->getConfiguration($config->getComponentId(), $config->getConfigurationId());

		$this->assertEquals($newName, $configuration['name'], 'Name should not be changed after description update');
		$this->assertEquals('neco', $configuration['description']);
		$this->assertEquals($configurationData, $configuration['configuration']);

		$config = (new \Keboola\StorageApi\Options\Components\Configuration())
			->setComponentId('gooddata-writer')
			->setConfigurationId('main-1')
			->setDescription('');

		$components->updateConfiguration($config);
		$configuration = $components->getConfiguration($config->getComponentId(), $config->getConfigurationId());
		$this->assertEquals('', $configuration['description'], 'Description can be set empty');
	}

	public function testComponentConfigsListShouldNotBeImplemented()
	{
		try {
			$this->_client->apiGet('storage/components/gooddata-writer/configs');
			$this->fail('Method should not be implemented');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals(501, $e->getCode());
			$this->assertEquals('notImplemented', $e->getStringCode());
		}
	}

	public function testListConfigs()
	{
		$components = new \Keboola\StorageApi\Components($this->_client);

		$configs = $components->listComponents();
		$this->assertEmpty($configs);


		$components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
				->setComponentId('gooddata-writer')
				->setConfigurationId('main-1')
				->setName('Main')
		);
		$components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
				->setComponentId('gooddata-writer')
				->setConfigurationId('main-2')
				->setConfiguration(array('x' => 'y'))
				->setName('Main')
		);
		$components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
				->setComponentId('provisioning')
				->setConfigurationId('main-1')
				->setName('Main')
		);

		$configs = $components->listComponents();
		$this->assertCount(2, $configs);

		$configs = $components->listComponents((new \Keboola\StorageApi\Options\Components\ListConfigurationsOptions())
			->setComponentType('writer'));

		$this->assertCount(2, $configs[0]['configurations']);
		$this->assertCount(1, $configs);

		$configuration = $configs[0]['configurations'][0];
		$this->assertArrayNotHasKey('configuration', $configuration);

		// list with configuration body
		$configs = $components->listComponents((new \Keboola\StorageApi\Options\Components\ListConfigurationsOptions())
			->setComponentType('writer')
			->setInclude(array('configuration'))
		);

		$this->assertCount(2, $configs[0]['configurations']);
		$this->assertCount(1, $configs);

		$configuration = $configs[0]['configurations'][0];
		$this->assertArrayHasKey('configuration', $configuration);
	}

	public function testDuplicateConfigShouldNotBeCreated()
	{
		$options = (new \Keboola\StorageApi\Options\Components\Configuration())
			->setComponentId('gooddata-writer')
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

		$client = new Keboola\StorageApi\Client(array(
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


}