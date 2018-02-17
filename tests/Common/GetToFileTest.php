<?php

namespace Keboola\Test\Common;

use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Nette\Utils\Json;

class GetToFileTest extends StorageApiTestCase
{
    private $downloadPath;

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
        $this->downloadPath = __DIR__ . '/../_tmp/downloaded.json';

        // cleanup
        $components = new Components($this->_client);
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

    public function testGetToFile()
    {
        // prepare data
        $config = new Configuration();
        $config->setComponentId('transformation');
        $config->setDescription('Test Configuration');
        $config->setConfigurationId('sapi-php-test');
        $config->setName('test-configuration');
        $component = new Components($this->_client);
        $configData = $component->addConfiguration($config);
        $config->setConfigurationId($configData['id']);

        $largeRowConfiguration = [
            'values' => []
        ];
        $valuesCount = 100;
        for ($i = 0; $i < $valuesCount; $i++) {
            $largeRowConfiguration['values'][] = sha1(random_bytes(128));
        }

        $configurationRowsCount = 100;
        for ($i = 0; $i < $configurationRowsCount; $i++) {
            $row = new ConfigurationRow($config);
            $row->setChangeDescription('Row 1');
            $row->setConfiguration($largeRowConfiguration);
            $component->addConfigurationRow($row);
        }

        // download
        $this->_client->apiGet('storage/components?include=configuration,rows,state', $this->downloadPath);

        $configurations =  \GuzzleHttp\json_decode(file_get_contents($this->downloadPath));
        $this->assertCount($configurationRowsCount, $configurations[0]->configurations[0]->rows);
    }
}
