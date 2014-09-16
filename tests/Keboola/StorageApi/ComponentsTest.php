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

	public function testComponentConfigUpdate()
	{
		$config = (new \Keboola\StorageApi\Options\Components\Configuration())
			->setComponentId('gooddata-writer')
			->setConfigurationId('main-1')
			->setName('Main');
		$components = new \Keboola\StorageApi\Components($this->_client);
		$components->addConfiguration($config);

		$newName = 'neco';
		$newDesc = 'some desc';
		$config->setName($newName)
			->setDescription($newDesc);
		$components->updateConfiguration($config);

		$list = $components->listComponents();
		$configuration = $list[0]['configurations'][0];

		$this->assertEquals($newName, $configuration['name']);
		$this->assertEquals($newDesc, $configuration['description']);
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