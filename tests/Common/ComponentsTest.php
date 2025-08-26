<?php
namespace Keboola\Test\Common;

use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Event;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ConfigurationRowState;
use Keboola\StorageApi\Options\Components\ConfigurationState;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationRowVersionsOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\EventsQueryBuilder;
use Symfony\Component\Console\Helper\Table;
use Symfony\Component\Console\Output\BufferedOutput;
use function json_decode;
use function var_dump;

class ComponentsTest extends StorageApiTestCase
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

    public function testPublicGetComponentDetail(): void
    {
        $componentId = 'wr-db';

        $componentsClient = new \Keboola\StorageApi\Components(new Client(
            ['url' => $this->client->getApiUrl(), 'token' => ''],
        ));
        $component = $componentsClient->getPublicComponentDetail($componentId);

        $this->assertEquals('wr-db', $component['id']);

        $this->assertArrayHasKey('id', $component);
        $this->assertArrayHasKey('uri', $component);
        $this->assertArrayHasKey('name', $component);
        $this->assertArrayHasKey('description', $component);
        $this->assertArrayHasKey('version', $component);
        $this->assertArrayHasKey('complexity', $component);
        $this->assertArrayHasKey('categories', $component);
        $this->assertArrayHasKey('hasRun', $component);
        $this->assertArrayHasKey('hasUI', $component);
        $this->assertArrayHasKey('ico32', $component);
        $this->assertArrayHasKey('ico64', $component);
        $this->assertArrayHasKey('ico128', $component);
        $this->assertArrayHasKey('type', $component);
        $this->assertArrayHasKey('data', $component);
        $this->assertArrayHasKey('flags', $component);
        $this->assertArrayHasKey('documentationUrl', $component);
        $this->assertArrayHasKey('longDescription', $component);
        $this->assertArrayHasKey('configurationSchema', $component);
        $this->assertArrayHasKey('configurationRowSchema', $component);
        $this->assertArrayHasKey('emptyConfiguration', $component);
        $this->assertArrayHasKey('emptyConfigurationRow', $component);
        $this->assertArrayHasKey('createConfigurationRowSchema', $component);
        $this->assertArrayHasKey('configurationDescription', $component);
        $this->assertArrayHasKey('uiOptions', $component);
        $this->assertArrayHasKey('features', $component);
        $this->assertArrayHasKey('expiredOn', $component);
        $this->assertArrayHasKey('dataTypesConfiguration', $component);
        $this->assertArrayHasKey('processorConfiguration', $component);

        $this->assertArrayNotHasKey('configurations', $component);
    }

    public function testDeprecatedUrlWithoutBranchIdStillWorks(): void
    {
        /** @var array $configuration */
        $configuration = $this->client->apiPostJson('components/wr-db/configs', [
            'name' => 'neco',
        ]);

        $components = new \Keboola\StorageApi\Components($this->client);
        $configuration = $components->getConfiguration('wr-db', $configuration['id']);
        $this->assertNotNull($configuration['description']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testGetComponentDetail(): void
    {
        $componentId = 'wr-db';
        $componentsClient = new \Keboola\StorageApi\Components($this->client);

        $component = $componentsClient->getComponent($componentId);

        $this->assertEquals('wr-db', $component['id']);

        $this->assertArrayHasKey('id', $component);
        $this->assertArrayHasKey('uri', $component);
        $this->assertArrayHasKey('name', $component);
        $this->assertArrayHasKey('description', $component);
        $this->assertArrayHasKey('version', $component);
        $this->assertArrayHasKey('complexity', $component);
        $this->assertArrayHasKey('categories', $component);
        $this->assertArrayHasKey('hasRun', $component);
        $this->assertArrayHasKey('hasUI', $component);
        $this->assertArrayHasKey('ico32', $component);
        $this->assertArrayHasKey('ico64', $component);
        $this->assertArrayHasKey('ico128', $component);
        $this->assertArrayHasKey('type', $component);
        $this->assertArrayHasKey('data', $component);
        $this->assertArrayHasKey('flags', $component);
        $this->assertArrayHasKey('documentationUrl', $component);
        $this->assertArrayHasKey('longDescription', $component);
        $this->assertArrayHasKey('configurationSchema', $component);
        $this->assertArrayHasKey('configurationRowSchema', $component);
        $this->assertArrayHasKey('emptyConfiguration', $component);
        $this->assertArrayHasKey('emptyConfigurationRow', $component);
        $this->assertArrayHasKey('createConfigurationRowSchema', $component);
        $this->assertArrayHasKey('configurationDescription', $component);
        $this->assertArrayHasKey('uiOptions', $component);
        $this->assertArrayHasKey('features', $component);
        $this->assertArrayHasKey('expiredOn', $component);
        $this->assertArrayHasKey('dataTypesConfiguration', $component);
        $this->assertArrayHasKey('processorConfiguration', $component);

        $this->assertArrayNotHasKey('configurations', $component);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testListComponents(): void
    {
        $componentId = 'wr-db';
        $configurationId = 'main-1';

        $componentsClient = new \Keboola\StorageApi\Components($this->client);
        $components = $componentsClient->listComponents(new ListComponentsOptions());

        $this->assertSame([], $components);

        // invalid include
        try {
            $componentsClient->listComponents((new ListComponentsOptions())->setInclude(['invalid']));
            $this->fail('List components with invalid include should fail');
        } catch (ClientException $e) {
            $this->assertSame(400, $e->getCode());
            $this->assertStringStartsWith('Invalid request', $e->getMessage());

            $params = $e->getContextParams();
            $this->assertArrayHasKey('errors', $params);

            $this->assertCount(1, $params['errors']);
            $error = reset($params['errors']);

            $this->assertSame(
                [
                    'key' => 'include',
                    'message' => 'Invalid include parameters: "invalid". Only following are allowed: configuration, rows, state.',
                ],
                $error,
            );
        }

        // create test configuration
        $configuration = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setState(['stateValue' => 'some-value'])
            ->setConfiguration(['value' => 1])
            ->setDescription('some desc')
        ;

        $componentsClient->addConfiguration($configuration);
        $componentsClient->addConfigurationRow(
            (new ConfigurationRow($configuration))
                ->setRowId('firstRow')
                ->setState(['rowStateValue' => 'some-value'])
                ->setConfiguration(['value' => 2]),
        );

        // list components without include
        $componentsNoInclude = $componentsClient->listComponents(new ListComponentsOptions());

        $this->assertCount(1, $componentsNoInclude);
        $component = reset($componentsNoInclude);

        $configuration = reset($component['configurations']);
        $this->assertArrayNotHasKey('configuration', $configuration);
        $this->assertArrayNotHasKey('rows', $configuration);
        $this->assertArrayNotHasKey('state', $configuration);

        // list components - rows include
        $componentsRows = $componentsClient->listComponents((new ListComponentsOptions())->setInclude(['rows']));

        $this->assertCount(1, $componentsRows);
        $component = reset($componentsRows);

        $configuration = reset($component['configurations']);
        $this->assertArrayNotHasKey('configuration', $configuration);
        $this->assertArrayHasKey('rows', $configuration);
        $this->assertArrayNotHasKey('state', $configuration);

        $row = reset($configuration['rows']);
        $this->assertArrayNotHasKey('configuration', $row);
        $this->assertArrayNotHasKey('state', $row);

        // list components - rows + state include
        $componentsRowsAndState = $componentsClient->listComponents((new ListComponentsOptions())->setInclude([
            'rows',
            'state',
        ]));

        $this->assertCount(1, $componentsRowsAndState);
        $component = reset($componentsRowsAndState);

        $configuration = reset($component['configurations']);
        $this->assertArrayNotHasKey('configuration', $configuration);
        $this->assertArrayHasKey('rows', $configuration);
        $this->assertArrayHasKey('state', $configuration);
        $this->assertSame(['stateValue' => 'some-value'], $configuration['state']);

        $row = reset($configuration['rows']);
        $this->assertArrayNotHasKey('configuration', $row);
        $this->assertArrayHasKey('state', $row);
        $this->assertSame(['rowStateValue' => 'some-value'], $row['state']);

        // list components - rows + configuration include
        $componentsRowsAndConfigurations = $componentsClient->listComponents((new ListComponentsOptions())->setInclude([
            'rows',
            'configuration',
        ]));

        $this->assertCount(1, $componentsRowsAndConfigurations);
        $component = reset($componentsRowsAndConfigurations);

        $configuration = reset($component['configurations']);
        $this->assertArrayHasKey('configuration', $configuration);
        $this->assertArrayHasKey('rows', $configuration);
        $this->assertArrayNotHasKey('state', $configuration);
        $this->assertSame(['value' => 1], $configuration['configuration']);

        $row = reset($configuration['rows']);
        $this->assertArrayHasKey('configuration', $row);
        $this->assertArrayNotHasKey('state', $row);
        $this->assertSame(['value' => 2], $row['configuration']);
    }

    /**
     * @dataProvider provideComponentsClientType
     * @group global-search
     */
    public function testComponentConfigRenew(string $devBranchType): void
    {
        $name = 'Main-'.$this->generateDescriptionForTestObject() . '-' . $devBranchType;
        $hashedName = sha1($name);
        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $components = new \Keboola\StorageApi\Components($this->client);

        $configuration = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName($hashedName)
            ->setDescription('some desc');
        $components->addConfiguration($configuration);

        $apiCall = fn() => $this->_client->globalSearch($hashedName);
        $assertCallback = function ($searchResult) use ($hashedName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $vuid1 = $components->getConfiguration($componentId, $configurationId)['currentVersion']['versionIdentifier'];
        $components->addConfigurationRow((new ConfigurationRow($configuration))
            ->setRowId('firstRow')
            ->setConfiguration(['value' => 1]));
        $vuid2 = $components->getConfiguration($componentId, $configurationId)['currentVersion']['versionIdentifier'];
        $this->assertNotEquals($vuid1, $vuid2);
        $components->deleteConfiguration($componentId, $configurationId);

        $apiCall = fn() => $this->_client->globalSearch($hashedName);
        $assertCallback = function ($searchResult) {
            $this->assertSame(0, $searchResult['all'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $nameRenewed = 'Main-'.$this->generateDescriptionForTestObject() . '-' . $devBranchType;
        $hashedNameRenewed = sha1($nameRenewed);
        // create configuration with same id as deleted
        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setConfiguration(['test' => false])
            ->setName($hashedNameRenewed)
            ->setDescription('some desc for renew'));

        $apiCall = fn() => $this->_client->globalSearch($hashedNameRenewed);
        $assertCallback = function ($searchResult) use ($hashedNameRenewed) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedNameRenewed, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $this->assertCount(0, $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)->setIsDeleted(true),
        ));

        $componentList = $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId),
        );
        $this->assertCount(1, $componentList);

        $component = reset($componentList);
        $this->assertEquals($configurationId, $component['id']);
        $this->assertEquals($hashedNameRenewed, $component['name']);
        $this->assertEquals('some desc for renew', $component['description']);
        $this->assertEquals(['test' => false], $component['configuration']);
        $this->assertSame('Configuration created', $component['changeDescription']);
        $this->assertFalse($component['isDeleted']);
        $this->assertEquals(4, $component['version']);
        $this->assertIsInt($component['version']);
        $this->assertIsInt($component['creatorToken']['id']);
        $this->assertCount(0, $component['rows']);
         $this->assertNotEquals($vuid2, $component['currentVersion']['versionIdentifier']);
    }

    /**
     * on defaultBranch only, no need devBranch because "Deleting configuration from trash is not allowed in
     * development branches."
     * @return void
     */
    public function testComponentConfigurationDelete(): void
    {
        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $components = new \Keboola\StorageApi\Components($this->client);

        $listComponentsConfigOptions = (new ListComponentConfigurationsOptions())->setComponentId($componentId);
        $listComponentsConfigOptionsDeleted = (new ListComponentConfigurationsOptions())
            ->setComponentId($componentId)
            ->setIsDeleted(true);

        $this->assertCount(0, $components->listComponentConfigurations($listComponentsConfigOptions));
        $this->assertCount(0, $components->listComponentConfigurations($listComponentsConfigOptionsDeleted));

        // add configuration
        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setDescription('some desc'));

        // delete configuration
        $components->deleteConfiguration($componentId, $configurationId);

        // check that configurations are empty
        $this->assertCount(0, $components->listComponentConfigurations($listComponentsConfigOptions));
        $this->assertCount(0, $components->listComponents());

        // check that there is one deleted configuration
        $componentList = $components->listComponentConfigurations($listComponentsConfigOptionsDeleted);
        $this->assertCount(1, $componentList);

        // check content of deleted configuration
        $component = reset($componentList);
        $this->assertEquals($configurationId, $component['id']);
        $this->assertEquals('Main', $component['name']);
        $this->assertEquals('some desc', $component['description']);
        $this->assertNotEmpty($component['changeDescription']);
        $this->assertTrue($component['isDeleted']);
        $this->assertEquals(2, $component['version']);
        $this->assertIsInt($component['version']);
        $this->assertIsInt($component['creatorToken']['id']);

        $currentVersion = $component['currentVersion'];
        $this->assertEquals('Configuration deleted', $currentVersion['changeDescription']);
        $tokenInfo = $this->_client->verifyToken();
        $this->assertEquals($tokenInfo['id'], $currentVersion['creatorToken']['id']);
        $this->assertEquals($tokenInfo['description'], $currentVersion['creatorToken']['description']);

        $componentsIndex = $components->listComponents((new ListComponentsOptions())->setIsDeleted(true));

        // check content of deleted component
        $this->assertCount(1, $componentsIndex);
        $this->assertArrayHasKey('id', $componentsIndex[0]);
        $this->assertArrayHasKey('configurations', $componentsIndex[0]);
        $this->assertEquals($componentId, $componentsIndex[0]['id']);
        $this->assertCount(1, $componentsIndex[0]['configurations']);

        // purge
        $components->deleteConfiguration($componentId, $configurationId);

        // it isn't present even as deleted
        $this->assertCount(0, $components->listComponentConfigurations($listComponentsConfigOptions));
        $this->assertCount(0, $components->listComponentConfigurations($listComponentsConfigOptionsDeleted));
    }

    /**
     * @dataProvider provideComponentsClientType
     * @param string $clientType
     * @group global-search
     */
    public function testComponentConfigRestore($clientType): void
    {
        $name = 'Main-'.$this->generateDescriptionForTestObject() . '-' . $clientType;
        $hashedName = sha1($name);

        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $components = new \Keboola\StorageApi\Components($this->client);

        $listConfigOptions = (new ListComponentConfigurationsOptions())->setComponentId($componentId);
        $listConfigOptionsDeleted = (new ListComponentConfigurationsOptions())
            ->setComponentId($componentId)
            ->setIsDeleted(true);
        $this->assertCount(0, $components->listComponentConfigurations($listConfigOptions));
        $this->assertCount(0, $components->listComponentConfigurations($listConfigOptionsDeleted));

        // add configuration
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName($hashedName)
            ->setDescription('some desc');
        $components->addConfiguration($config
            ->setIsDisabled(true));

        $apiCall = fn() => $this->_client->globalSearch($hashedName);
        $assertCallback = function ($searchResult) use ($hashedName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $vuid1 = $components->getConfiguration($componentId, $configurationId)['currentVersion']['versionIdentifier'];
        // add configuration row
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $components->addConfigurationRow($configurationRow);
        $vuid2 = $components->getConfiguration($componentId, $configurationId)['currentVersion']['versionIdentifier'];
        $this->assertNotEquals($vuid1, $vuid2);

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId));
        $this->assertCount(1, $rows);

        $configurations = $components->listComponentConfigurations($listConfigOptions);
        $this->assertCount(1, $configurations);

        // delete configuration
        $components->deleteConfiguration($componentId, $configurationId);

        $apiCall = fn() => $this->_client->globalSearch($hashedName);
        $assertCallback = function ($searchResult) {
            $this->assertSame(0, $searchResult['all'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $listConfigurationOptions = (new ListComponentConfigurationsOptions())->setComponentId($componentId);
        $configurations = $components->listComponentConfigurations($listConfigOptions);
        $this->assertCount(0, $configurations);

        $listConfigurationOptions->setIsDeleted(true);
        $configurations = $components->listComponentConfigurations($listConfigOptionsDeleted);
        $this->assertCount(1, $configurations);

        // restore deleted configuration
        $components->restoreComponentConfiguration($componentId, $configurationId);

        $apiCall = fn() => $this->_client->globalSearch($hashedName);
        $assertCallback = function ($searchResult) use ($hashedName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $this->assertCount(0, $components->listComponentConfigurations($listConfigOptionsDeleted));

        $this->assertCount(1, $components->listConfigurationRows(
            (new ListConfigurationRowsOptions())->setComponentId($componentId)
                ->setConfigurationId($config->getConfigurationId()),
        ));

        $componentList = $components->listComponentConfigurations($listConfigOptions);
        $this->assertCount(1, $componentList);

        $component = reset($componentList);
        $this->assertEquals($configurationId, $component['id']);
        $this->assertEquals($hashedName, $component['name']);
        $this->assertEquals('some desc', $component['description']);
        $this->assertSame('Configuration restored', $component['changeDescription']);
        $this->assertTrue($component['isDisabled']);
        $this->assertFalse($component['isDeleted']);
        $this->assertEquals(4, $component['version']);
        $this->assertIsInt($component['version']);
        $this->assertIsInt($component['creatorToken']['id']);
        $vuid3 = $component['currentVersion']['versionIdentifier'];
        $this->assertNotEquals($vuid2, $vuid3);

        // try to restore again
        try {
            $components->restoreComponentConfiguration($componentId, $configurationId);
            $this->fail('Configuration should not be restored again');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
//            $this->assertSame('notFound', $e->getStringCode());
//            $this->assertStringContainsString('Deleted configuration main-1 not found', $e->getMessage());
        }

        // delete configuration again
        $components->deleteConfiguration($componentId, $configurationId);

        $configurations = $components->listComponentConfigurations($listConfigOptions);
        $this->assertCount(0, $configurations);

        $configurations = $components->listComponentConfigurations($listConfigOptionsDeleted);
        $this->assertCount(1, $configurations);

        try {
            $components->listConfigurationRows((new ListConfigurationRowsOptions())
                ->setComponentId($componentId)
                ->setConfigurationId($configurationId));
            $this->fail('Configuration rows for deleted configuration should not be listed');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertStringContainsString('Configuration main-1 not found', $e->getMessage());
        }

        // restore configuration with create same configuration id and test number of rows
        $configurationRestored = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($config->getConfigurationId())
            ->setConfiguration(['a' => 'b'])
            ->setChangeDescription('Config restored...')
            ->setName('Main 1 restored');
        $components->addConfiguration($configurationRestored);
        $this->assertCount(0, $components->listComponentConfigurations($listConfigOptionsDeleted));
        $this->assertCount(0, $components->listConfigurationRows(
            (new ListConfigurationRowsOptions())->setComponentId($componentId)
                ->setConfigurationId($configurationRestored->getConfigurationId()),
        ));

        $configuration = $components->getConfiguration($componentId, 'main-1');
        $this->assertSame('main-1', $configuration['id']);
        $this->assertSame('Main 1 restored', $configuration['name']);
        $this->assertSame(['a' => 'b'], $configuration['configuration']);
        $this->assertSame('Config restored...', $configuration['changeDescription']);
        $this->assertFalse($configuration['isDisabled']);
        $this->assertSame(6, $configuration['version']);
         $this->assertNotEquals($vuid3, $configuration['currentVersion']['versionIdentifier']);
    }

    /**
     * @dataProvider provideComponentsClientType
     * @group global-search
     */
    public function testComponentConfigCreate(string $devBranchType): void
    {
        $name = 'Main-'.$this->generateDescriptionForTestObject() . '-' . $devBranchType;
        $hashedName = sha1($name);
        $components = new \Keboola\StorageApi\Components($this->client);
        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName($hashedName)
            ->setDescription('some desc'));

        $component = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals($hashedName, $component['name']);
        $this->assertEquals('some desc', $component['description']);
        $this->assertEmpty($component['configuration']);
        $this->assertSame('Configuration created', $component['changeDescription']);
        $this->assertEquals(1, $component['version']);
        $this->assertIsInt($component['version']);
        $this->assertIsInt($component['creatorToken']['id']);
        $this->assertNotEmpty($component['currentVersion']['versionIdentifier']);

        $apiCall = fn() => $this->_client->globalSearch($hashedName);
        $assertCallback = function ($searchResult) use ($hashedName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $components = $components->listComponents();
        $this->assertCount(1, $components);

        $component = reset($components);
        $this->assertEquals('wr-db', $component['id']);
        $this->assertCount(1, $component['configurations']);

        $configuration = reset($component['configurations']);
        $this->assertEquals('main-1', $configuration['id']);
        $this->assertEquals($hashedName, $configuration['name']);
        $this->assertEquals('some desc', $configuration['description']);
         $this->assertNotEmpty($configuration['currentVersion']['versionIdentifier']);
    }

    /**
     * @dataProvider provideComponentsClientType
     * @param string $clientType
     * @return void
     */
    public function testComponentConfigIsDisabled($clientType): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);

        // create configuration with isDisabled = true
        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setIsDisabled(true));

        // check created
        $configuration = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(1, $configuration['version']);
        $this->assertTrue($configuration['isDisabled']);

        $vuid1 = $configuration['currentVersion']['versionIdentifier'];

        $componentList = $components->listComponents();
        $this->assertCount(1, $componentList);

        $component = reset($componentList);
        $this->assertEquals('wr-db', $component['id']);
        $this->assertCount(1, $component['configurations']);

        $configuration = reset($component['configurations']);
        $this->assertEquals(1, $configuration['version']);
        $this->assertTrue($configuration['isDisabled']);

        // update config with isDisabled = false (version 2)
        $components->updateConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setIsDisabled(false));

        $configuration = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(2, $configuration['version']);
        $this->assertFalse($configuration['isDisabled']);
        $vuid2 = $configuration['currentVersion']['versionIdentifier'];
        $this->assertNotSame($vuid1, $vuid2);

        // rollback config to version 1 (version 3)
        $components->rollbackConfiguration('wr-db', 'main-1', 1);

        $configuration = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(3, $configuration['version']);
        $this->assertTrue($configuration['isDisabled']);
        $vuid3 = $configuration['currentVersion']['versionIdentifier'];
        $this->assertNotSame($vuid2, $vuid3);
    }

    /**
     * @dataProvider isDisabledMixedProvider
     * @param string $clientType
     * @param mixed $isDisabled
     * @param bool $expectedIsDisabled
     * @return void
     */
    public function testComponentConfigCreateIsDisabledMixed($clientType, $isDisabled, $expectedIsDisabled): void
    {
        // create config
        $client = $this->clientProvider->createGuzzleClientForCurrentTest([
            'base_uri' => $this->client->getApiUrl(),
        ], true);

        $response = $client->post('/v2/storage/branch/default/components/wr-db/configs', [
            'json' => [
                'name' => 'test configuration',
                'isDisabled' => $isDisabled,
            ],
            'headers' => [
                'X-StorageApi-Token' => $this->client->getTokenString(),
            ],
        ]);
        /** @var \stdClass $response */
        $response = json_decode((string) $response->getBody());

        $this->assertEquals('test configuration', $response->name);
        $this->assertEquals($expectedIsDisabled, $response->isDisabled);
    }

    /**
     * @return \Generator
     */
    public function isDisabledMixedProvider()
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

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigRestrictionsForReadOnlyUser(): void
    {
        $readOnlyClient = $this->clientProvider->createClientForCurrentTest([
            'token' => STORAGE_API_READ_ONLY_TOKEN,
            'url' => STORAGE_API_URL,
        ], true);

        $configuration = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc')
        ;

        $componentsForAdmin = new \Keboola\StorageApi\Components($this->client);
        $componentsForAdmin->addConfiguration($configuration);

        $components = $componentsForAdmin->listComponents();
        $this->assertCount(1, $components);

        $componentsForReadOnlyUser = new \Keboola\StorageApi\Components($readOnlyClient);

        $this->assertSame($components, $componentsForReadOnlyUser->listComponents());

        try {
            $componentsForReadOnlyUser->addConfiguration($configuration);
            $this->fail('Components API POST request should be restricted for readOnly user');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('accessDenied', $e->getStringCode());
            $this->assertStringContainsString('You don\'t have access to the resource.', $e->getMessage());
        }

        try {
            $configuration->setName('Renamed');
            $componentsForReadOnlyUser->updateConfiguration($configuration);
            $this->fail('Components API PUT request should be restricted for readOnly user');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('accessDenied', $e->getStringCode());
            $this->assertStringContainsString('You don\'t have access to the resource.', $e->getMessage());
        }

        try {
            $componentsForReadOnlyUser->deleteConfiguration($configuration->getComponentId(), $configuration->getConfigurationId());
            $this->fail('Components API PUT request should be restricted for readOnly user');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('accessDenied', $e->getStringCode());
            $this->assertStringContainsString('You don\'t have access to the resource.', $e->getMessage());
        }

        $this->assertSame($components, $componentsForAdmin->listComponents());
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testConfigurationNameShouldBeRequired(): void
    {
        try {
            $branchPrefix = '';
            if (!$this->client instanceof BranchAwareClient) {
                $branchPrefix = 'branch/default/';
            }
            $this->client->apiPostJson($branchPrefix . 'components/wr-db/configs', []);
            $this->fail('Params should be invalid');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.components.validation', $e->getStringCode());
            $this->assertStringContainsString('name', $e->getMessage());
        }
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testConfigurationDescriptionDefault(): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);

        $branchPrefix = '';
        if (!$this->client instanceof BranchAwareClient) {
            $branchPrefix = 'branch/default/';
        }
        $resp = $this->client->apiPostJson($branchPrefix . 'components/wr-db/configs', [
            'name' => 'neco',
        ]);
        $configuration = $components->getConfiguration('wr-db', $resp['id']);
        $this->assertNotNull($configuration['description']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testNonJsonConfigurationShouldNotBeAllowed(): void
    {
        try {
            $branchPrefix = '';
            if (!$this->client instanceof BranchAwareClient) {
                $branchPrefix = 'branch/default/';
            }
            $this->client->apiPostJson($branchPrefix . 'components/wr-db/configs', [
                'name' => 'neco',
                'description' => 'some',
                'configuration' => '{sdf}',
            ]);
            $this->fail('Post invalid json should not be allowed.');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('validation.invalidConfigurationFormat', $e->getStringCode());
        }
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testNonJsonStateShouldNotBeAllowed(): void
    {
        try {
            $branchPrefix = '';
            if (!$this->client instanceof BranchAwareClient) {
                $branchPrefix = 'branch/default/';
            }
            $this->client->apiPostJson($branchPrefix . 'components/wr-db/configs', [
                'name' => 'neco',
                'description' => 'some',
                'state' => '{sdf}',
            ]);
            $this->fail('Post invalid json should not be allowed.');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('validation.invalidStateFormat', $e->getStringCode());
        }
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigurationJsonDataTypes(): void
    {
        // to check if params is object we have to convert received json to objects instead of assoc array
        // so we have to use raw Http Client
        $client = $this->clientProvider->createGuzzleClientForCurrentTest([
            'base_uri' => $this->_client->getApiUrl(),
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

        $response = $client->post('/v2/storage/branch/default/components/wr-db/configs', [
            'json' => [
                'name' => 'test',
                'configuration' => json_encode($config),
                'state' => json_encode($state),
            ],
            'headers' => [
                'X-StorageApi-Token' => $this->_client->getTokenString(),
            ],
        ]);
        $response = json_decode((string) $response->getBody());
        $this->assertEquals($config, $response->configuration);
        $this->assertEquals($state, $response->state);

        $response = $client->get("/v2/storage/branch/default/components/wr-db/configs/{$response->id}", [
            'headers' => [
                'X-StorageApi-Token' => $this->_client->getTokenString(),
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
        $response = $client->put("/v2/storage/branch/default/components/wr-db/configs/{$response->id}", [
            'json' => [
                'configuration' => json_encode($config),
            ],
            'headers' => [
                'X-StorageApi-Token' => $this->_client->getTokenString(),
            ],
        ]);
        $response = json_decode((string) $response->getBody());
        $this->assertEquals($config, $response->configuration);

        $response = $client->get("/v2/storage/branch/default/components/wr-db/configs/{$response->id}", [
            'headers' => [
                'X-StorageApi-Token' => $this->_client->getTokenString(),
            ],
        ]);
        $response = json_decode((string) $response->getBody());
        $this->assertEquals($config, $response->configuration);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigCreateWithConfigurationJson(): void
    {
        $configuration = [
            'queries' => [
                [
                    'id' => 1,
                    'query' => 'SELECT * from some_table',
                ],
            ],
        ];

        $components = new \Keboola\StorageApi\Components($this->client);
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

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigCreateWithStateJson(): void
    {
        $state = [
            'queries' => [
                [
                    'id' => 1,
                    'query' => 'SELECT * from some_table',
                ],
            ],
        ];
        $components = new \Keboola\StorageApi\Components($this->client);
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

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigUpdateEmptyStateJson(): void
    {
        $state = [
            'queries' => [
                [
                    'id' => 1,
                    'query' => 'SELECT * from some_table',
                ],
            ],
        ];
        $components = new \Keboola\StorageApi\Components($this->client);
        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc')
            ->setState($state));

        $config = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals($state, $config['state']);
        $this->assertEquals(1, $config['version']);

        $components->updateConfigurationState((new ConfigurationState())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setState([]));

        $config = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals([], $config['state']);
        $this->assertEquals(1, $config['version']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigCreateIdAutoCreate(): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);
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

    /**
     * @dataProvider provideComponentsClientType
     * @param string $devBranchType
     * @group global-search
     */
    public function testComponentConfigUpdate(string $devBranchType): void
    {
        $name = 'Main-'. $this->generateDescriptionForTestObject() . '-' . $devBranchType;
        $hashedName = sha1($name);
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setDescription($name)
            ->setName(sha1($name));
        $components = new \Keboola\StorageApi\Components($this->client);
        $newConfiguration = $components->addConfiguration($config);
        $this->assertEquals(1, $newConfiguration['version']);
        $this->assertEmpty($newConfiguration['state']);

        $apiCall = fn() => $this->_client->globalSearch($hashedName);
        $assertCallback = function ($searchResult) use ($hashedName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $vuid1 = $newConfiguration['currentVersion']['versionIdentifier'];
        $newName = 'neco-'.$this->generateDescriptionForTestObject() . '-' . $devBranchType;
        $hashedNewName = sha1($newName);
        $newDesc = 'some desc';
        $configurationData = ['x' => 'y'];
        $config->setName(sha1($newName))
            ->setDescription($newDesc)
            ->setConfiguration($configurationData);
        $components->updateConfiguration($config);

        $apiCall = fn() => $this->_client->globalSearch($hashedNewName);
        $assertCallback = function ($searchResult) use ($hashedNewName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedNewName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $configuration = $components->getConfiguration($config->getComponentId(), $config->getConfigurationId());

        $vuid2 = $configuration['currentVersion']['versionIdentifier'];
        $this->assertNotSame($vuid1, $vuid2);

        $this->assertEquals($hashedNewName, $configuration['name']);
        $this->assertEquals($newDesc, $configuration['description']);
        $this->assertEquals($config->getConfiguration(), $configuration['configuration']);
        $this->assertEquals(2, $configuration['version']);
        $this->assertEquals('Configuration updated', $configuration['changeDescription']);
        $this->assertFalse($configuration['isDisabled']);

        $state = [
            'cache' => true,
        ];
        $updatedConfig = $components->updateConfigurationState((new ConfigurationState())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setState($state));
        $this->assertEquals($hashedNewName, $updatedConfig['name'], 'Name should not be changed after description update');
        $this->assertEquals('some desc', $updatedConfig['description']);
        $this->assertEquals($configurationData, $updatedConfig['configuration']);
        $this->assertEquals($state, $updatedConfig['state']);
        $this->assertEquals('Configuration updated', $updatedConfig['changeDescription']);
        $this->assertFalse($updatedConfig['isDisabled']);

        $configuration = $components->getConfiguration($config->getComponentId(), $config->getConfigurationId());

        $vuid3 = $configuration['currentVersion']['versionIdentifier'];
        $this->assertSame($vuid2, $vuid3, 'State update should not create new version');

        $this->assertEquals($hashedNewName, $configuration['name'], 'Name should not be changed after description update');
        $this->assertEquals('some desc', $configuration['description']);
        $this->assertEquals($configurationData, $configuration['configuration']);
        $this->assertEquals($state, $configuration['state']);
        $this->assertEquals('Configuration updated', $configuration['changeDescription']);
        $this->assertFalse($configuration['isDisabled']);

        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setDescription('');

        $components->updateConfiguration($config);
        $configuration = $components->getConfiguration($config->getComponentId(), $config->getConfigurationId());
        $this->assertEquals('', $configuration['description'], 'Description can be set empty');

        $vuid4 = $configuration['currentVersion']['versionIdentifier'];
        $this->assertNotSame($vuid3, $vuid4);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigUpdateConfigEmpty(): void
    {
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components = new \Keboola\StorageApi\Components($this->client);
        $newConfiguration = $components->addConfiguration($config);
        $this->assertEquals(1, $newConfiguration['version']);
        $this->assertEmpty($newConfiguration['state']);

        $newName = 'neco';
        $newDesc = 'some desc';
        $configurationData = ['foo' => 'bar'];
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
        $this->assertSame('Configuration updated', $updatedConfig['changeDescription']);
        $this->assertEquals(3, $updatedConfig['version']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigUpdateEmptyWithEmpty(): void
    {
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components = new \Keboola\StorageApi\Components($this->client);
        $newConfiguration = $components->addConfiguration($config);
        $this->assertEquals(1, $newConfiguration['version']);
        $this->assertEmpty($newConfiguration['configuration']);

        $vuid1 = $newConfiguration['currentVersion']['versionIdentifier'];

        $config->setConfiguration([]);
        $components->updateConfiguration($config);

        $updatedConfig = $components->getConfiguration($config->getComponentId(), $config->getConfigurationId());
        $this->assertEquals(1, $updatedConfig['version'], 'there should not be any change');
         $this->assertSame($vuid1, $updatedConfig['currentVersion']['versionIdentifier']);

        $components->updateConfiguration($config);
        $updatedConfig = $components->getConfiguration($config->getComponentId(), $config->getConfigurationId());
        $this->assertEquals(1, $updatedConfig['version'], 'there should not be any change');
         $this->assertSame($vuid1, $updatedConfig['currentVersion']['versionIdentifier']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigUpdateWithRows(): void
    {
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components = new \Keboola\StorageApi\Components($this->client);
        $newConfiguration = $components->addConfiguration($config);
        $this->assertEquals(1, $newConfiguration['version']);
        $this->assertEmpty($newConfiguration['state']);

        $newName = 'neco';
        $newDesc = 'some desc';
        $configurationData = ['x' => 'y'];
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

        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setDescription('neco');

        $updatedConfig = $components->updateConfiguration($config);
        $this->assertEquals($newName, $updatedConfig['name'], 'Name should not be changed after description update');
        $this->assertEquals($configurationData, $updatedConfig['configuration']);

        $this->assertArrayHasKey('rows', $updatedConfig);
        $this->assertCount(1, $updatedConfig['rows']);

        $row = reset($updatedConfig['rows']);
        $this->assertEquals('main-1-1', $row['id']);

        $configuration = $components->getConfiguration($config->getComponentId(), $config->getConfigurationId());

        $this->assertEquals($newName, $configuration['name'], 'Name should not be changed after description update');
        $this->assertEquals($configurationData, $configuration['configuration']);

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

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigUpdateVersioning(): void
    {
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components = new \Keboola\StorageApi\Components($this->client);
        $newConfiguration = $components->addConfiguration($config);
        $this->assertEquals(1, $newConfiguration['version']);
        $this->assertEmpty($newConfiguration['state']);
        $vuid1 = $newConfiguration['currentVersion']['versionIdentifier'];

        $listConfig = (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
            ->setComponentId($config->getComponentId())
            ->setConfigurationId($config->getConfigurationId())
            ->setInclude(['name', 'state']);
        $versions = $components->listConfigurationVersions($listConfig);
        $this->assertCount(1, $versions, 'Configuration should have one version');

        $newName = 'neco';
        $newDesc = 'some desc';
        $configurationData = ['x' => 'y'];
        $config->setName($newName)
            ->setDescription($newDesc)
            ->setConfiguration($configurationData);
        $components->updateConfiguration($config);
        $versions = $components->listConfigurationVersions($listConfig);
        $this->assertCount(2, $versions, 'Update of configuration name should add version');
        $lastVersion = reset($versions);
        $this->assertEquals(2, $lastVersion['version']);
        $vuid2 = $lastVersion['versionIdentifier'];
        $this->assertNotSame($vuid1, $vuid2);

        $state = ['cache' => true];
        $components->updateConfigurationState((new ConfigurationState())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setState($state));
        $versions = $components->listConfigurationVersions($listConfig);
        $this->assertCount(2, $versions, 'Update of configuration state should not add version');
        $lastVersion = reset($versions);
        $this->assertEquals(2, $lastVersion['version']);
        $vuid3 = $lastVersion['versionIdentifier'];
        $this->assertSame($vuid2, $vuid3);

        $components->updateConfiguration($config);
        $versions = $components->listConfigurationVersions($listConfig);
        $this->assertCount(2, $versions, 'Update without change should not add version');
        $lastVersion = reset($versions);
        $this->assertEquals(2, $lastVersion['version']);
         $this->assertSame($vuid2, $lastVersion['versionIdentifier']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigUpdateChangeDescription(): void
    {
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components = new \Keboola\StorageApi\Components($this->client);
        $newConfiguration = $components->addConfiguration($config);
        $this->assertEquals(1, $newConfiguration['version']);
        $this->assertEmpty($newConfiguration['state']);

        $changeDesc = 'change Description';
        $newName = 'neco';
        $newDesc = 'some desc';
        $configurationData = ['x' => 'y'];
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
            ->setInclude(['name', 'state']);
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
        $this->assertEquals('Configuration updated', $secondConfigLoaded['changeDescription']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigsVersionsList(): void
    {
        $configuration = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $componentsApi = new \Keboola\StorageApi\Components($this->client);
        $newConfiguration = $componentsApi->addConfiguration($configuration);

        // create version 2
        $newName = 'neco';
        $newDesc = 'some desc';
        $configurationData = ['x' => 'y'];
        $configuration->setName($newName)
            ->setDescription($newDesc)
            ->setConfiguration($configurationData);
        $componentsApi->updateConfiguration($configuration);
        $configuration2 = $componentsApi->getConfiguration(
            $configuration->getComponentId(),
            $configuration->getConfigurationId(),
        );

        $configuration = (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
            ->setComponentId($configuration->getComponentId())
            ->setConfigurationId($configuration->getConfigurationId())
            ->setInclude(['name', 'state']);
        $configurationVersions = $componentsApi->listConfigurationVersions($configuration);

        $this->assertCount(2, $configurationVersions);

        $latestConfigurationVersion = $configurationVersions[0];
        $previousConfigurationVersion = $configurationVersions[1];

        $this->assertArrayHasKey('version', $latestConfigurationVersion);
         $this->assertArrayHasKey('versionIdentifier', $latestConfigurationVersion);
        $this->assertSame(2, $latestConfigurationVersion['version']);
        $this->assertArrayHasKey('name', $latestConfigurationVersion);
        $this->assertSame('neco', $latestConfigurationVersion['name']);
        $this->assertArrayNotHasKey('state', $latestConfigurationVersion);
        $this->assertArrayNotHasKey('description', $latestConfigurationVersion);

        $this->assertArrayHasKey('version', $previousConfigurationVersion);
        $this->assertSame(1, $previousConfigurationVersion['version']);
        $this->assertArrayHasKey('name', $previousConfigurationVersion);
        $this->assertSame('Main', $previousConfigurationVersion['name']);

        $this->assertSame(
            $configuration2['currentVersion']['changeDescription'],
            $latestConfigurationVersion['changeDescription'],
        );
        $this->assertSame(
            $configuration2['currentVersion']['creatorToken'],
            $latestConfigurationVersion['creatorToken'],
        );
        $this->assertSame(
            $configuration2['currentVersion']['created'],
            $latestConfigurationVersion['created'],
        );

        $configuration = (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
            ->setComponentId($configuration->getComponentId())
            ->setConfigurationId($configuration->getConfigurationId())
            ->setInclude(['name', 'configuration'])
            ->setOffset(0)
            ->setLimit(1);
        $configurationVersionsWithLimit = $componentsApi->listConfigurationVersions($configuration);

        $this->assertCount(1, $configurationVersionsWithLimit);
        $this->assertArrayHasKey('version', $configurationVersionsWithLimit[0]);
        $this->assertSame(2, $configurationVersionsWithLimit[0]['version']);
        $this->assertArrayNotHasKey('state', $configurationVersionsWithLimit[0]);
        $this->assertArrayHasKey('configuration', $configurationVersionsWithLimit[0]);
        $this->assertSame($configurationData, $configurationVersionsWithLimit[0]['configuration']);

        $configuration = (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
            ->setComponentId($configuration->getComponentId())
            ->setConfigurationId($configuration->getConfigurationId());
        $configurationVersion = $componentsApi->getConfigurationVersion($configuration->getComponentId(), $configuration->getConfigurationId(), 2);

        $this->assertArrayHasKey('version', $configurationVersion);
        $this->assertSame(2, $configurationVersion['version']);
        $this->assertIsInt($configurationVersion['creatorToken']['id']);
        $this->assertArrayNotHasKey('state', $configurationVersion);
        $this->assertArrayHasKey('configuration', $configurationVersion);
        $this->assertSame($configurationData, $configurationVersion['configuration']);
        $configurationVersion = $componentsApi->listConfigurationVersions($configuration);
        $this->assertCount(2, $configurationVersion);
    }

    /**
     * Create configuration with few rows, update some row and then rollback to configuration with updated row
     * @dataProvider provideComponentsClientType
     * @param string $clientName
     */
    public function testConfigurationRollback($clientName): void
    {
        $this->initEvents($this->client);
        $componentsApi = new \Keboola\StorageApi\Components($this->client);

        // create configuration
        $configuration = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setConfiguration(['a' => 'b'])
            ->setName('Main');
        $configurationV1 = $componentsApi->addConfiguration($configuration);
        $vuid1 = $configurationV1['currentVersion']['versionIdentifier'];

        // add first row - conf V2
        $configurationRowOptions = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRowOptions->setConfiguration(['first' => 1]);
        $configurationRow1 = $componentsApi->addConfigurationRow($configurationRowOptions);

        $configurationV2 = $componentsApi->getConfiguration('wr-db', $configurationV1['id']);
        $vuid2 = $configurationV2['currentVersion']['versionIdentifier'];
        $this->assertNotSame($vuid1, $vuid2);

        // add another row  - conf V3
        $configurationRowOptions = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRowOptions->setConfiguration(['second' => 1]);
        $componentsApi->addConfigurationRow($configurationRowOptions);

        $configurationV3 = $componentsApi->getConfiguration('wr-db', $configurationV1['id']);
        $vuid3 = $configurationV3['currentVersion']['versionIdentifier'];
        $this->assertNotSame($vuid2, $vuid3);

        // update first row
        $configurationRowOptions = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRowOptions->setConfiguration(['first' => 22])->setRowId($configurationRow1['id']);
        $componentsApi->updateConfigurationRow($configurationRowOptions);

        $configurationV4 = $componentsApi->getConfiguration('wr-db', $configurationV1['id']);
        $vuid4 = $configurationV4['currentVersion']['versionIdentifier'];

        // update config - conf V5
        $componentsApi->updateConfiguration($configuration->setConfiguration(['d' => 'b']));
        $configurationV5 = $componentsApi->getConfiguration('wr-db', $configurationV1['id']);

        $vuid5 = $configurationV5['currentVersion']['versionIdentifier'];
        // wait a moment, rollbacked version should have different created date
        sleep(2);

        // rollback to version 2 - conf V6
        // second row should be missing, and first row should be rolled back to first version
        $componentsApi->rollbackConfiguration('wr-db', $configurationV1['id'], 2);

        if ($clientName === ClientProvider::DEV_BRANCH) {
            /** @var BranchAwareClient $branchClient */
            $branchClient = $this->client;
            $assertCallback = function ($events) use ($branchClient) {
                $this->assertCount(1, $events);
                $this->assertEquals($branchClient->getCurrentBranchId(), $events[0]['idBranch']);
            };
            $query = new EventsQueryBuilder();
            $query->setEvent('storage.componentConfigurationRolledBack')
                ->setComponent('storage');
            $this->assertEventWithRetries($this->client, $assertCallback, $query);
        }

        $rollbackedConfiguration = $componentsApi->getConfiguration('wr-db', $configurationV1['id']);

        $vuid6 = $rollbackedConfiguration['currentVersion']['versionIdentifier'];
        $this->assertNotSame($vuid5, $vuid6);
        $this->assertNotSame($vuid2, $vuid6);

        // asserts about the configuration itself
        $this->assertEquals(6, $rollbackedConfiguration['version'], 'Rollback added new configuration version');
        $this->assertEquals('Rollback to version 2', $rollbackedConfiguration['changeDescription']);
        $this->assertCount(1, $rollbackedConfiguration['rows']);
        $this->assertEquals('Rollback to version 2', $rollbackedConfiguration['currentVersion']['changeDescription']);
        $this->assertArrayEqualsExceptKeys(
            $configurationV2['currentVersion'],
            $rollbackedConfiguration['currentVersion'],
            [
                'created',
                'changeDescription',
                'versionIdentifier',
            ],
        );
        $this->assertArrayEqualsExceptKeys($configurationV2, $rollbackedConfiguration, [
            'version',
            'changeDescription',
            'rows',
            'currentVersion',
        ]);

        // asserts about configuration's rows
        $this->assertCount(1, $rollbackedConfiguration['rows']);
        $rollbackedRow = $rollbackedConfiguration['rows'][0];
        $this->assertEquals(3, $rollbackedRow['version']);
        $this->assertEquals(
            'Rollback to version 1 (via configuration rollback to version 2)',
            $rollbackedRow['changeDescription'],
        );
        $this->assertArrayEqualsExceptKeys($configurationRow1, $rollbackedRow, [
            'version',
            'changeDescription',
        ]);

        // rollback to version 5 - conf V7
        $componentsApi->rollbackConfiguration('wr-db', $configurationV1['id'], 5, 'custom description');

        if ($clientName === ClientProvider::DEV_BRANCH) {
            /** @var BranchAwareClient $branchClient */
            $branchClient = $this->client;
            $assertCallback = function ($events) use ($branchClient) {
                $this->assertCount(2, $events);
                $this->assertEquals($branchClient->getCurrentBranchId(), $events[0]['idBranch']);
            };
            $query = new EventsQueryBuilder();
            $query->setEvent('storage.componentConfigurationRolledBack')
                ->setComponent('storage');
            $this->assertEventWithRetries($this->client, $assertCallback, $query);
        }

        $rollbackedConfiguration = $componentsApi->getConfiguration('wr-db', $configurationV1['id']);
        $vuid7 = $rollbackedConfiguration['currentVersion']['versionIdentifier'];
        $this->assertNotSame($vuid6, $vuid7);
        $this->assertNotSame($vuid5, $vuid7);
        // asserts about the configuration itself
        $this->assertEquals(7, $rollbackedConfiguration['version'], 'Rollback added new configuration version');
        $this->assertEquals('custom description', $rollbackedConfiguration['changeDescription']);
        $this->assertCount(2, $rollbackedConfiguration['rows']);
        $this->assertEquals('custom description', $rollbackedConfiguration['currentVersion']['changeDescription']);
        $this->assertArrayEqualsExceptKeys(
            $configurationV5['currentVersion'],
            $rollbackedConfiguration['currentVersion'],
            [
                'created',
                'changeDescription',
                'versionIdentifier',
            ],
        );
        $this->assertArrayEqualsExceptKeys($configurationV5, $rollbackedConfiguration, [
            'version',
            'changeDescription',
            'rows',
            'currentVersion',
        ]);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testUpdateRowWithoutIdShouldNotBeAllowed(): void
    {
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components = new \Keboola\StorageApi\Components($this->client);
        $newConfiguration = $components->addConfiguration($config);

        // add first row
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $firstRowConfig = ['first' => 1];
        $configurationRow->setConfiguration($firstRowConfig);
        $firstRow = $components->addConfigurationRow($configurationRow);

        // update row
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $configurationRow->setConfiguration(['first' => 'dd']);
        try {
            $components->updateConfigurationRow($configurationRow);
            $this->fail('Should have thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(400, $e->getCode(), 'User error should be thrown');
        }
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testUpdateConfigWithoutIdShouldNotBeAllowed(): void
    {
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components = new \Keboola\StorageApi\Components($this->client);
        $newConfiguration = $components->addConfiguration($config);

        $config->setConfigurationId(null);

        try {
            $components->updateConfiguration($config);
            $this->fail('Should have thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(400, $e->getCode(), 'User error should be thrown');
        }
    }

    /**
     * @dataProvider provideComponentsClientType
     * @group global-search
     */
    public function testComponentConfigsVersionsRollback(string $devBranchType): void
    {
        $name = 'Main-'.$this->generateDescriptionForTestObject() . '-' . $devBranchType;
        $hashedName = sha1($name);

        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName($hashedName);
        $components = new \Keboola\StorageApi\Components($this->client);
        $newConfiguration = $components->addConfiguration($config);
        $this->assertEquals(1, $newConfiguration['version']);
        $this->assertEmpty($newConfiguration['state']);

        $apiCall = fn() => $this->_client->globalSearch($hashedName);
        $assertCallback = function ($searchResult) use ($hashedName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $configurationRow->setRowId('main-1-1')
            ->setConfiguration(['first' => 1]);

        $components->addConfigurationRow($configurationRow);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $configurationRow->setRowId('main-1-2')
            ->setConfiguration(['second' => 1]);

        $components->addConfigurationRow($configurationRow);

        $listOptions = new ListComponentsOptions();
        $listOptions->setInclude(['rows']);
        $components = $components->listComponents($listOptions);

        $this->assertCount(1, $components);

        $component = reset($components);
        $configuration = reset($component['configurations']);

        $this->assertArrayHasKey('rows', $configuration);
        $this->assertCount(2, $configuration['rows']);

        $components = new \Keboola\StorageApi\Components($this->client);

        $newName = 'neco-'.$this->generateDescriptionForTestObject() . '-' . $devBranchType;
        $newHashedName = sha1($newName);
        $newDesc = 'some desc';
        $configurationData = ['x' => 'y'];
        $config->setName($newHashedName)
            ->setDescription($newDesc)
            ->setConfiguration($configurationData);
        $components->updateConfiguration($config);

        $apiCall = fn() => $this->_client->globalSearch($newHashedName);
        $assertCallback = function ($searchResult) use ($newHashedName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($newHashedName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $config = (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
            ->setComponentId($config->getComponentId())
            ->setConfigurationId($config->getConfigurationId());
        $result = $components->rollbackConfiguration($config->getComponentId(), $config->getConfigurationId(), 2);

        $apiCall = fn() => $this->_client->globalSearch($hashedName);
        $assertCallback = function ($searchResult) use ($hashedName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $this->assertArrayHasKey('version', $result);
        $this->assertEquals(5, $result['version']);
        $result = $components->getConfigurationVersion($config->getComponentId(), $config->getConfigurationId(), 3);
        $this->assertArrayHasKey('name', $result);
        $this->assertEquals($hashedName, $result['name']);
        $result = $components->listConfigurationVersions($config);
        $this->assertCount(5, $result);

        $listOptions = new ListComponentsOptions();
        $listOptions->setInclude(['rows']);
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

    /**
     * @dataProvider provideComponentsClientType
     * @group global-search
     */
    public function testComponentConfigsVersionsCreate(string $devBranchType): void
    {
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components = new \Keboola\StorageApi\Components($this->client);
        $newConfiguration = $components->addConfiguration($config);
        $this->assertEquals(1, $newConfiguration['version']);
        $this->assertEmpty($newConfiguration['state']);

        // update config - version incremented to 2
        $newName = 'neco';
        $newDesc = 'some desc';
        $configurationData = ['x' => 'y'];
        $config->setName($newName)
            ->setDescription($newDesc)
            ->setConfiguration($configurationData)
            ->setIsDisabled(true);
        $components->updateConfiguration($config);

        // add 1st row - version incremented to 3
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $configurationRow->setRowId('main-1-1');
        $components->addConfigurationRow($configurationRow);

        // add 2nd row - version incremented to 4
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $configurationRow->setRowId('main-1-2');
        $components->addConfigurationRow($configurationRow);

        // add 3rd row - version incremented to 5
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $configurationRow->setRowId('main-1-3');
        $components->addConfigurationRow($configurationRow);

        // rollback to version 4 (with 2 rows)
        $name = 'New-'.$this->generateDescriptionForTestObject() . '-' . $devBranchType;
        $hashedName = sha1($name);
        $result = $components->createConfigurationFromVersion($config->getComponentId(), $config->getConfigurationId(), 4, $hashedName);
        $apiCall = fn() => $this->_client->globalSearch($hashedName);
        $assertCallback = function ($searchResult) use ($hashedName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);
        $this->assertArrayHasKey('id', $result);
        $configuration = $components->getConfiguration($config->getComponentId(), $result['id']);
        $this->assertArrayHasKey('name', $configuration);
        $this->assertEquals($hashedName, $configuration['name']);
        $this->assertArrayHasKey('description', $configuration);
        $this->assertEquals($newDesc, $configuration['description']);
        $this->assertArrayHasKey('version', $configuration);
        $this->assertEquals(1, $configuration['version']);
        $this->assertArrayHasKey('configuration', $configuration);
        $this->assertEquals($configurationData, $configuration['configuration']);
        $this->assertArrayHasKey('isDisabled', $configuration);
        $this->assertTrue($configuration['isDisabled']);
        // check rows
        $this->assertArrayHasKey('rows', $configuration);
        $this->assertCount(2, $configuration['rows']);
        $this->assertEquals('main-1-1', $configuration['rows'][0]['id']);
        $this->assertEquals('main-1-2', $configuration['rows'][1]['id']);

        // rollback to version 1 (with 0 rows)
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
        $this->assertArrayHasKey('isDisabled', $configuration);
        $this->assertFalse($configuration['isDisabled']);
        // check rows
        $this->assertArrayHasKey('rows', $configuration);
        $this->assertCount(0, $configuration['rows']);
    }

    /**
     * @dataProvider provideComponentsClientType
     * @return void
     */
    public function testVersionIncreaseWhenUpdate(): void
    {
        $componentsApi = new \Keboola\StorageApi\Components($this->client);

        $configuration = new Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $componentsApi->addConfiguration($configuration);

        $configurationRow = new ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');
        $configurationRow->setConfiguration([
            'my-value' => 666,
        ]);
        $componentsApi->addConfigurationRow($configurationRow);

        $configurationRow = new ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-2');
        $configurationRow->setConfiguration([
            'my-value' => 666,
        ]);
        $componentsApi->addConfigurationRow($configurationRow);

        $componentConfiguration = $componentsApi->getConfiguration('wr-db', 'main-1');

        $this->assertEquals(3, $componentConfiguration['version']);

        $configuration = new Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setRowsSortOrder(['main-1-1', 'main-1-2']);
        $componentsApi->updateConfiguration($configuration);

        $componentConfiguration = $componentsApi->getConfiguration('wr-db', 'main-1');

        $this->assertEquals(4, $componentConfiguration['version']);

        // calling the update once again without any change, the version should remain
        $configuration = new Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setRowsSortOrder(['main-1-1', 'main-1-2']);
        $componentsApi->updateConfiguration($configuration);

        $componentConfiguration = $componentsApi->getConfiguration('wr-db', 'main-1');

        $this->assertEquals(4, $componentConfiguration['version']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testListConfigs(): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);

        $configs = $components->listComponents();
        $this->assertEmpty($configs);

        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main'));
        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-2')
            ->setConfiguration(['x' => 'y'])
            ->setName('Main'));
        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('provisioning')
            ->setConfigurationId('main-1')
            ->setName('Main'));

        $configs = $components->listComponents();
        $this->assertCount(2, $configs);

        $configs = $components->listComponents((new ListComponentsOptions())
            ->setComponentType('writer'));

        $this->assertCount(2, $configs[0]['configurations']);
        $this->assertCount(1, $configs);

        $configuration = $configs[0]['configurations'][0];
        $this->assertArrayNotHasKey('configuration', $configuration);

        // list with configuration body
        $configs = $components->listComponents((new ListComponentsOptions())
            ->setComponentType('writer')
            ->setInclude(['configuration']));

        $this->assertCount(2, $configs[0]['configurations']);
        $this->assertCount(1, $configs);

        $configuration = $configs[0]['configurations'][0];
        $this->assertArrayHasKey('configuration', $configuration);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testDuplicateConfigShouldNotBeCreated(): void
    {
        $options = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');

        $components = new \Keboola\StorageApi\Components($this->client);
        $components->addConfiguration($options);

        // $this->expectException ???
        try {
            $components->addConfiguration($options);
            $this->fail('Configuration should not be created');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('configurationAlreadyExists', $e->getStringCode());
        }
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testPermissions(): void
    {
        $tokenOptions = (new TokenCreateOptions())
            ->setDescription('test')
        ;

        $token = $this->tokens->createToken($tokenOptions);

        $client = $this->clientProvider->createClientForCurrentTest([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ], true);

        $components = new \Keboola\StorageApi\Components($client);

        try {
            $components->listComponents();
            $this->fail('List components should not be allowed');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testTokenWithComponentAccess(): void
    {
        $this->_initEmptyTestBuckets();

        $tokenOptions = (new TokenCreateOptions())
            ->setDescription('test components')
            ->addComponentAccess('provisioning')
        ;

        $token = $this->tokens->createToken($tokenOptions);

        $client = $this->clientProvider->createClientForCurrentTest([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ], true);

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
            $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
                ->setComponentId('wr-db')
                ->setName('Main'));
            $this->fail('Have not been granted permission to access this component, should throw exception');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        $this->tokens->dropToken($token['id']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testTokenWithManageAllBucketsShouldHaveAccessToComponents(): void
    {
        $tokenOptions = (new TokenCreateOptions())
            ->setDescription('test components')
            ->setCanManageBuckets(true)
        ;

        $token = $this->tokens->createToken($tokenOptions);

        $client = $this->clientProvider->createClientForCurrentTest([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ], true);

        $components = new \Keboola\StorageApi\Components($client);

        $componentsList = $components->listComponents();
        $this->assertEmpty($componentsList);

        $config = $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setName('Main'));

        $componentsList = $components->listComponents();
        $this->assertCount(1, $componentsList);

        $this->assertEquals($config['id'], $componentsList[0]['configurations'][0]['id']);
        $this->tokens->dropToken($token['id']);
    }

    /**
     * @dataProvider provideComponentsClientType
     * @group global-search
     */
    public function testComponentConfigRowCreate(string $devBranchType): void
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc');

        $components = new \Keboola\StorageApi\Components($this->client);

        $components->addConfiguration($configuration);

        $component = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals('Main', $component['name']);
        $this->assertEquals('some desc', $component['description']);
        $this->assertEmpty($component['configuration']);
        $this->assertEquals(1, $component['version']);
        $this->assertIsInt($component['version']);
        $this->assertIsInt($component['creatorToken']['id']);

        $rowName = 'main-1-1-'.$this->generateDescriptionForTestObject() . '-' . $devBranchType;
        $hashedRowName = sha1($rowName);
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');
        $configurationRow->setName($hashedRowName);

        $components->addConfigurationRow($configurationRow);

        $apiCall = fn() => $this->_client->globalSearch($hashedRowName);
        $assertCallback = function ($searchResult) use ($hashedRowName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration-row', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedRowName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };

        $this->retryWithCallback($apiCall, $assertCallback);
        $listOptions = new ListComponentsOptions();
        $listOptions->setInclude(['rows']);
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
        $this->assertEquals($hashedRowName, $row['name']);
        $this->assertEquals('', $row['description']);
        $this->assertEquals(false, $row['isDisabled']);

        $components = new \Keboola\StorageApi\Components($this->client);

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
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

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigRowCreateName(): void
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('main');

        $components = new \Keboola\StorageApi\Components($this->client);
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow
            ->setRowId('main-1-1')
            ->setName('row name')
        ;

        $components->addConfigurationRow($configurationRow);

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1'));

        $row = reset($rows);
        $this->assertEquals('main-1-1', $row['id']);
        $this->assertEquals('row name', $row['name']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigRowCreateDescription(): void
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('main');

        $components = new \Keboola\StorageApi\Components($this->client);
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow
            ->setRowId('main-1-1')
            ->setDescription('row description')
        ;

        $components->addConfigurationRow($configurationRow);

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1'));

        $row = reset($rows);
        $this->assertEquals('main-1-1', $row['id']);
        $this->assertEquals('row description', $row['description']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigRowCreateIsDisabled(): void
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('main');

        $components = new \Keboola\StorageApi\Components($this->client);
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow
            ->setRowId('main-1-1')
            ->setIsDisabled(true)
        ;

        $components->addConfigurationRow($configurationRow);

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1'));

        $row = reset($rows);
        $this->assertEquals('main-1-1', $row['id']);
        $this->assertEquals(true, $row['isDisabled']);
    }

    /**
     * @dataProvider provideComponentsClientType
     * @group global-search
     */
    public function testComponentConfigRowUpdateName(string $devBranchType): void
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('main');

        $components = new \Keboola\StorageApi\Components($this->client);
        $components->addConfiguration($configuration);

        $rowConfigurationData = [
            'some' => 'configuration',
        ];
        $rowName = 'main-1-1'.$this->generateDescriptionForTestObject() . '-' . $devBranchType;
        $hashedRowName = sha1($rowName);

        $rowDescription = 'some description';
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow
            ->setRowId('main-1-1')
            ->setName($hashedRowName)
            ->setConfiguration($rowConfigurationData)
            ->setDescription($rowDescription);
        ;

        $components->addConfigurationRow($configurationRow);

        $apiCall = fn() => $this->_client->globalSearch($hashedRowName);
        $assertCallback = function ($searchResult) use ($hashedRowName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration-row', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedRowName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $updatedRowName = 'updated-main-1-1'.$this->generateDescriptionForTestObject() . '-' . $devBranchType;
        $hashedUpdatedRowName = sha1($updatedRowName);

        $updateConfigurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $updateConfigurationRow
            ->setRowId('main-1-1')
            ->setName($hashedUpdatedRowName)
        ;
        $components->updateConfigurationRow($updateConfigurationRow);

        $apiCall = fn() => $this->_client->globalSearch($hashedRowName);
        $assertCallback = function ($searchResult) {
            $this->assertSame(0, $searchResult['all'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $apiCall = fn() => $this->_client->globalSearch($hashedUpdatedRowName);
        $assertCallback = function ($searchResult) use ($hashedUpdatedRowName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration-row', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedUpdatedRowName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1'));

        $row = reset($rows);
        $this->assertEquals($hashedUpdatedRowName, $row['name']);
        $this->assertEquals($rowConfigurationData, $row['configuration']);
        $this->assertEquals($rowDescription, $row['description']);

        $configuration = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(3, $configuration['version']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigRowUpdateDescription(): void
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('main');

        $components = new \Keboola\StorageApi\Components($this->client);
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

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1'));

        $row = reset($rows);
        $this->assertEquals('altered row description', $row['description']);

        $configuration = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(3, $configuration['version']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigRowUpdateIsDisabled(): void
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('main');

        $components = new \Keboola\StorageApi\Components($this->client);
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow
            ->setRowId('main-1-1')
        ;

        $components->addConfigurationRow($configurationRow);

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
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

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1'));

        $row = reset($rows);
        $this->assertEquals(true, $row['isDisabled']);

        $configuration = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(3, $configuration['version']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigRowUpdateConfigEmpty(): void
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('main')
        ;

        $components = new \Keboola\StorageApi\Components($this->client);
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow
            ->setRowId('main-1-1')
            ->setConfiguration(['foo' => 'bar'])
        ;
        $components->addConfigurationRow($configurationRow);

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
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

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1'));

        $row = reset($rows);
        $this->assertEquals([], $row['configuration']);

        $configuration = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(3, $configuration['version']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigRowUpdateNoNewVersionIsCreatedIfNothingChanged(): void
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('main')
        ;

        $componentsApi = new \Keboola\StorageApi\Components($this->client);
        $componentsApi->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');
        $componentsApi->addConfigurationRow($configurationRow);

        // nothing is changed
        $componentsApi->updateConfigurationRow($configurationRow);

        $rows = $componentsApi->listConfigurationRowVersions(
            (new ListConfigurationRowVersionsOptions())
                ->setComponentId('wr-db')
                ->setConfigurationId('main-1')
                ->setRowId($configurationRow->getRowId()),
        );
        $this->assertCount(1, $rows);
        $this->assertSame(1, $rows[0]['version']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigRowUpdateConfigEmptyWithEmpty(): void
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('main')
        ;

        $components = new \Keboola\StorageApi\Components($this->client);
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow
            ->setRowId('main-1-1');
        ;
        $components->addConfigurationRow($configurationRow);

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
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

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1'));

        $row = reset($rows);
        $this->assertEmpty($row['configuration']);
        $this->assertEquals(1, $row['version']);
    }

    /**
     * @dataProvider provideComponentsClientType
     * @group global-search
     */
    public function testComponentConfigRowUpdate(string $devBranchType): void
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc');

        // use default client for default branch because verify token is not implemented for dev branch
        $originalToken = $this->_client->verifyToken();

        $components = new \Keboola\StorageApi\Components($this->client);

        $components->addConfiguration($configuration);

        $component = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals('Main', $component['name']);
        $this->assertEquals('some desc', $component['description']);
        $this->assertEmpty($component['configuration']);
        $this->assertEquals(1, $component['version']);
        $this->assertIsInt($component['version']);
        $this->assertIsInt($component['creatorToken']['id']);

        $rowName = 'main-1-1'.$this->generateDescriptionForTestObject() . '-' . $devBranchType;
        $hashedRowName = sha1($rowName);
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');
        $configurationRow->setName($hashedRowName);

        $components->addConfigurationRow($configurationRow);

        $apiCall = fn() => $this->_client->globalSearch($hashedRowName);
        $assertCallback = function ($searchResult) use ($hashedRowName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration-row', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedRowName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $listOptions = new ListComponentsOptions();
        $listOptions->setInclude(['rows']);
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

        $components = new \Keboola\StorageApi\Components($this->client);

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($component['id'])
            ->setConfigurationId($configuration['id']));

        $originalRow = reset($rows);
        $this->assertEquals('main-1-1', $originalRow['id']);
        $this->assertEquals('Row main-1-1 added', $originalRow['changeDescription']);

        $component = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(2, $component['version']);

        $row = $components->updateConfigurationRow($configurationRow);

        $this->assertEquals(2, $row['version']);
        $this->assertEmpty($row['configuration']);

        $configurationData = ['test' => 1];
        $configurationChangeDescription = 'Change description test';

        $configurationRow->setConfiguration($configurationData)
            ->setChangeDescription($configurationChangeDescription);

        $tokenOptions = (new TokenCreateOptions())
            ->setDescription('test')
            ->setExpiresIn(60)
            ->addComponentAccess('wr-db')
        ;

        $newToken = $this->tokens->createToken($tokenOptions);

        $newClient = $this->clientProvider->createClientForCurrentTest([
            'token' => $newToken['token'],
            'url' => STORAGE_API_URL,
        ], true);

        $newComponents = new \Keboola\StorageApi\Components($newClient);
        $row = $newComponents->updateConfigurationRow($configurationRow);
        $configurationAssociatedWithUpdatedRow = $newComponents->getConfiguration('wr-db', 'main-1');

        $this->assertEquals(3, $row['version']);
        $this->assertEquals($configurationData, $row['configuration']);
        $this->assertEquals($originalRow['created'], $row['created'], 'row created data should not be changed');
        $this->assertEquals($configurationChangeDescription, $row['changeDescription']);
        $this->assertEquals(
            $configurationChangeDescription,
            $configurationAssociatedWithUpdatedRow['changeDescription'],
        );

        $version = $components->getConfigurationRowVersion(
            $configurationRow->getComponentConfiguration()->getComponentId(),
            $configurationRow->getComponentConfiguration()->getConfigurationId(),
            $configurationRow->getRowId(),
            3,
        );

        $this->assertArrayHasKey('changeDescription', $version);
        $this->assertEquals('Change description test', $version['changeDescription']);
        $this->assertNotEmpty($version['created']);
        $this->assertEquals($newToken['id'], $version['creatorToken']['id']);
        $this->assertEquals($newToken['description'], $version['creatorToken']['description']);

        $renamedRowName = 'renamed-main-1-1'.$this->generateDescriptionForTestObject() . '-' . $devBranchType;
        $hashedRenamedRowName = sha1($renamedRowName);
        $components->updateConfigurationRow(
            $configurationRow
                ->setName($hashedRenamedRowName)
                ->setChangeDescription(null),
        );

        $apiCall = fn() => $this->_client->globalSearch($hashedRenamedRowName);
        $assertCallback = function ($searchResult) use ($hashedRenamedRowName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration-row', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedRenamedRowName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $updatedRow = $components->getConfigurationRow(
            'wr-db',
            'main-1',
            'main-1-1',
        );
        $configurationAssociatedWithUpdatedRow = $newComponents->getConfiguration('wr-db', 'main-1');

        $this->assertEquals('Row main-1-1 changed', $updatedRow['changeDescription']);
        $this->assertEquals('Row main-1-1 changed', $configurationAssociatedWithUpdatedRow['changeDescription']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigRowStateUpdate(): void
    {
        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $rowId = 'main-1-1';

        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setDescription('some desc');

        $components = new \Keboola\StorageApi\Components($this->client);

        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId($rowId);

        $components->addConfigurationRow($configurationRow);

        $state = [
            'my' => 'test',
        ];

        $rowState = (new ConfigurationRowState($configuration))
            ->setRowId($rowId)
            ->setState($state)
        ;

        $row = $components->updateConfigurationRowState($rowState);

        $this->assertSame($state, $row['state']);
        $this->assertSame(1, $row['version']);
        $this->assertSame(
            $row,
            $components->getConfigurationRow(
                $rowState->getComponentConfiguration()->getComponentId(),
                $rowState->getComponentConfiguration()->getConfigurationId(),
                $rowState->getRowId(),
            ),
        );

        $branchPrefix = '';
        if (!$this->client instanceof BranchAwareClient) {
            $branchPrefix = 'branch/default/';
        }
        $stateEndpoint = $branchPrefix . "components/{$componentId}/configs/{$configurationId}/rows/{$rowId}/state";

        try {
            $this->client->apiPutJson($stateEndpoint, [
                'state' => '{sdf}',
            ]);
            $this->fail('Post invalid json should not be allowed.');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('validation.invalidStateFormat', $e->getStringCode());
            $this->assertEquals('Invalid state body format: This value should be valid JSON.', $e->getMessage());
        }

        try {
            $this->client->apiPutJson($stateEndpoint, [
                'description' => 'Test',
                'state' => json_encode('{}'),
            ]);
            $this->fail('Post additional fileds should not be allowed.');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.componentsRows.validation', $e->getStringCode());
            $this->assertEquals('Invalid parameters - description: This field was not expected.', $e->getMessage());
        }

        try {
            $this->client->apiPutJson($stateEndpoint, [
                'state' => '',
            ]);
            $this->fail('Post empty state should not be allowed.');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('validation.invalidStateFormat', $e->getStringCode());
            $this->assertEquals('Invalid state body format: This value should not be blank.', $e->getMessage());
        }

        try {
            $this->client->apiPutJson($stateEndpoint, []);
            $this->fail('Post without state should not be allowed.');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('validation.invalidStateFormat', $e->getStringCode());
            $this->assertEquals('Invalid state body format: This field is missing.', $e->getMessage());
        }

        $this->assertSame(
            $row,
            $components->getConfigurationRow(
                $rowState->getComponentConfiguration()->getComponentId(),
                $rowState->getComponentConfiguration()->getConfigurationId(),
                $rowState->getRowId(),
            ),
        );
    }

    /**
     * @dataProvider provideComponentsClientType
     * @group global-search
     */
    public function testComponentConfigRowDelete(string $devBranchType): void
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc');

        $components = new \Keboola\StorageApi\Components($this->client);

        $components->addConfiguration($configuration);

        $component = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals('Main', $component['name']);
        $this->assertEquals('some desc', $component['description']);
        $this->assertEmpty($component['configuration']);
        $this->assertEquals(1, $component['version']);
        $this->assertIsInt($component['version']);
        $this->assertIsInt($component['creatorToken']['id']);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);

        $configurationRow->setRowId('main-1-1');

        $components->addConfigurationRow($configurationRow);

        $rowName = 'main-1-2'.$this->generateDescriptionForTestObject() . '-' . $devBranchType;
        $hashedRowName = sha1($rowName);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-2');
        $configurationRow->setName($hashedRowName);

        $components->addConfigurationRow($configurationRow);

        $apiCall = fn() => $this->_client->globalSearch($hashedRowName);
        $assertCallback = function ($searchResult) use ($hashedRowName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration-row', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedRowName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $listOptions = new ListComponentsOptions();
        $listOptions->setInclude(['rows']);
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

        $components = new \Keboola\StorageApi\Components($this->client);

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
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
            $configurationRow->getRowId(),
        );

        $apiCall = fn() => $this->_client->globalSearch($hashedRowName);
        $assertCallback = function ($searchResult) {
            $this->assertSame(0, $searchResult['all'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $components = new \Keboola\StorageApi\Components($this->client);

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($configurationRow->getComponentConfiguration()->getComponentId())
            ->setConfigurationId($configurationRow->getComponentConfiguration()->getConfigurationId()));

        $this->assertCount(1, $rows);

        $row = reset($rows);
        $this->assertEquals('main-1-1', $row['id']);

        $component = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(4, $component['version']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigDeletedRowId(): void
    {
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('transformation')
            ->setConfigurationId('main')
            ->setName('Main');
        $components = new \Keboola\StorageApi\Components($this->client);
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow
            ->setRowId('test')
            ->setConfiguration(['key' => 'value']);
        $components->addConfigurationRow($configurationRow);
        $components->deleteConfigurationRow('transformation', 'main', 'test');
        $components->addConfigurationRow($configurationRow->setConfiguration(['key' => 'newValue']));

        $listRowsOptions = new ListConfigurationRowsOptions();
        $listRowsOptions
            ->setComponentId('transformation')
            ->setConfigurationId('main');
        $rows = $components->listConfigurationRows($listRowsOptions);
        $this->assertCount(1, $rows);

        $row = reset($rows);
        $this->assertEquals(2, $row['version']);
        $this->assertEquals(['key' => 'newValue'], $row['configuration']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigRowsListAndConfigRowVersionsList(): void
    {
        $componentsApi = new \Keboola\StorageApi\Components($this->client);

        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc');
        $componentsApi->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');
        $row1 = $componentsApi->addConfigurationRow($configurationRow);

        $rows = $componentsApi->listConfigurationRows(
            (new ListConfigurationRowsOptions())
                ->setComponentId('wr-db')
                ->setConfigurationId('main-1'),
        );

        $this->assertCount(1, $rows);
        $this->assertEquals($row1, $rows[0]);

        $configurationData = ['test' => 1];
        $configurationRow->setConfiguration($configurationData);
        $row2 = $componentsApi->updateConfigurationRow($configurationRow);

        $versions = $componentsApi->listConfigurationRowVersions(
            (new ListConfigurationRowVersionsOptions())
                ->setComponentId('wr-db')
                ->setConfigurationId('main-1')
                ->setRowId($configurationRow->getRowId()),
        );

        $this->assertCount(2, $versions);
        $exceptKeys = [
            'id', // is not in the response
            'created', // in version it shows when version was created, not row
            'configuration', // not included
            'state', // not included
        ];
        $this->assertArrayEqualsIgnoreKeys($row2, $versions[0], $exceptKeys);
        $this->assertArrayEqualsIgnoreKeys($row1, $versions[1], $exceptKeys);

        $versionsWithConfiguration = $componentsApi->listConfigurationRowVersions(
            (new ListConfigurationRowVersionsOptions())
                ->setComponentId('wr-db')
                ->setConfigurationId('main-1')
                ->setRowId($configurationRow->getRowId())
                // intentionally added "state" that is not supported
                // it should be silently dropped
                ->setInclude(['configuration', 'state']),
        );

        $this->assertCount(2, $versionsWithConfiguration);

        $exceptKeys = [
            'id', // is not in the response
            'created', // in version it shows when version was created, not row
            'state', // not included
        ];
        $this->assertArrayEqualsIgnoreKeys($row2, $versionsWithConfiguration[0], $exceptKeys);
        $this->assertArrayEqualsIgnoreKeys($row1, $versionsWithConfiguration[1], $exceptKeys);

        foreach ($versionsWithConfiguration as $version) {
            $rowVersion = $componentsApi->getConfigurationRowVersion(
                'wr-db',
                'main-1',
                $configurationRow->getRowId(),
                $version['version'],
            );

            $this->assertEquals($rowVersion, $version);
        }

        $versionsWithLimitAndOffset = $componentsApi->listConfigurationRowVersions(
            (new ListConfigurationRowVersionsOptions())
                ->setComponentId('wr-db')
                ->setConfigurationId('main-1')
                ->setRowId($configurationRow->getRowId())
                ->setInclude(['configuration'])
                ->setLimit(1)
                ->setOffset(1),
        );

        $this->assertCount(1, $versionsWithLimitAndOffset);

        $rowVersion = $componentsApi->getConfigurationRowVersion(
            'wr-db',
            'main-1',
            $configurationRow->getRowId(),
            1,
        );
        $this->assertEquals($rowVersion, $versionsWithLimitAndOffset[0]);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigRowVersionRollback(): void
    {
        $componentsApi = new \Keboola\StorageApi\Components($this->client);

        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc');
        $componentsApi->addConfiguration($configuration);

        $configurationRowV1 = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRowV1->setRowId('main-1-1');
        $configurationRowV1->setConfiguration([
            'my-value' => 666,
        ]);
        $componentsApi->addConfigurationRow($configurationRowV1);

        $componentsApi->getConfiguration('wr-db', 'main-1');

        // update row 1st - without change
        $componentsApi->updateConfigurationRow($configurationRowV1);

        // update row V2
        $configurationRowV1
            ->setConfiguration([
                'test' => 1,
            ])
            ->setChangeDescription('some change');
        $configurationRowV2 = $componentsApi->updateConfigurationRow($configurationRowV1);

        // update row V3
        $configurationRowV1
            ->setConfiguration([
                'test' => 2,
            ])
            ->setChangeDescription(null);
        $configurationRowV3 = $componentsApi->updateConfigurationRow($configurationRowV1);

        // rollback to V2 -> V4
        $configurationRowV4 = $componentsApi->rollbackConfigurationRow(
            'wr-db',
            'main-1',
            $configurationRowV1->getRowId(),
            2,
        );

        $this->assertEquals(4, $configurationRowV4['version'], 'Rollback creates new version of the configuration');
        $this->assertEquals(
            'Rollback to version 2',
            $configurationRowV4['changeDescription'],
            'Rollback creates automatic description',
        );
        $this->assertArrayEqualsExceptKeys($configurationRowV2, $configurationRowV4, [
            'version',
            'changeDescription',
        ]);

        // try same assert but load row from api
        $configurationRowV4 = $componentsApi->getConfigurationRow(
            'wr-db',
            'main-1',
            $configurationRowV1->getRowId(),
        );
        $this->assertEquals(4, $configurationRowV4['version'], 'Rollback creates new version of the configuration');
        $this->assertEquals(
            'Rollback to version 2',
            $configurationRowV4['changeDescription'],
            'Rollback creates automatic description',
        );
        $this->assertArrayEqualsExceptKeys($configurationRowV2, $configurationRowV4, [
            'version',
            'changeDescription',
        ]);

        $configuration = $componentsApi->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(5, $configuration['version']);
        $this->assertEquals(
            'Row main-1-1 version 2 rollback',
            $configuration['changeDescription'],
            'Rollback creates automatic description',
        );

        // rollback to version 3
        $configurationRowV5 = $componentsApi->rollbackConfigurationRow(
            'wr-db',
            'main-1',
            $configurationRowV1->getRowId(),
            3,
            'Custom rollback message',
        );

        $this->assertEquals(5, $configurationRowV5['version'], 'Rollback creates new version of the row');
        $this->assertEquals('Custom rollback message', $configurationRowV5['changeDescription']);
        $this->assertArrayEqualsExceptKeys($configurationRowV3, $configurationRowV5, ['version', 'changeDescription']);

        $configuration = $componentsApi->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(6, $configuration['version']);
        $this->assertEquals(
            'Custom rollback message',
            $configuration['changeDescription'],
            'Rollback creates automatic description',
        );

        $versions = $componentsApi->listConfigurationRowVersions(
            (new ListConfigurationRowVersionsOptions())
                ->setComponentId('wr-db')
                ->setConfigurationId('main-1')
                ->setRowId($configurationRowV1->getRowId()),
        );

        $this->assertCount(5, $versions);
    }

    /**
     * @dataProvider provideComponentsClientType
     * @group global-search
     */
    public function testComponentConfigRowVersionCreate(string $devBranchType): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);

        $configurationData = ['my-value' => 666];

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

        $rowName = 'main-1-1'.$this->generateDescriptionForTestObject() . '-' . $devBranchType;
        $hashedRowName = sha1($rowName);
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');
        $configurationRow->setConfiguration($configurationData);
        $configurationRow->setName($hashedRowName);

        $components->addConfigurationRow($configurationRow);

        $apiCall = fn() => $this->_client->globalSearch($hashedRowName);
        $assertCallback = function ($searchResult) use ($hashedRowName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration-row', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedRowName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        // copy to same first configuration
        $row = $components->createConfigurationRowFromVersion(
            $configuration->getComponentId(),
            $configuration->getConfigurationId(),
            'main-1-1',
            1,
        );

        $apiCall = fn() => $this->_client->globalSearch($hashedRowName);
        $assertCallback = function ($searchResult) use ($hashedRowName) {
            $this->assertSame(2, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration-row', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedRowName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $this->assertArrayHasKey('id', $row);
        $this->assertArrayHasKey('version', $row);
        $this->assertArrayHasKey('configuration', $row);

        $this->assertEquals(1, $row['version']);
        $this->assertEquals($configurationData, $row['configuration']);
        $this->assertEquals($hashedRowName, $row['name']);
        $this->assertEquals('', $row['description']);
        $this->assertEquals(false, $row['isDisabled']);

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($configuration->getComponentId())
            ->setConfigurationId($configuration->getConfigurationId()));

        $this->assertCount(2, $rows);

        // copy to same second configuration
        $row = $components->createConfigurationRowFromVersion(
            $configuration->getComponentId(),
            $configuration->getConfigurationId(),
            'main-1-1',
            1,
            $configuration2->getConfigurationId(),
        );

        $apiCall = fn() => $this->_client->globalSearch($hashedRowName);
        $assertCallback = function ($searchResult) use ($hashedRowName) {
            $this->assertSame(3, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration-row', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedRowName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
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

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($configuration2->getComponentId())
            ->setConfigurationId($configuration2->getConfigurationId()));

        $this->assertCount(1, $rows);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testComponentConfigRowVersionCreateWithEmptyRowId(): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);

        $configurationData = ['my-value' => 666];

        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId(100) // use numeric id
            ->setName('Main')
            ->setDescription('some desc');

        $components->addConfiguration($configuration);

        $configuration2 = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration2
            ->setComponentId($configuration->getComponentId())
            ->setConfigurationId(200) // use numeric id
            ->setName('Main')
            ->setDescription('some desc');

        $components->addConfiguration($configuration2);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId(101); // use numeric id
        $configurationRow->setConfiguration($configurationData);

        $components->addConfigurationRow($configurationRow);

        // copy to same first configuration
        $row = $components->createConfigurationRowFromVersion(
            $configuration->getComponentId(),
            $configuration->getConfigurationId(),
            101,
            1,
        );

        $this->assertArrayHasKey('id', $row);
        $this->assertArrayHasKey('version', $row);
        $this->assertArrayHasKey('configuration', $row);

        $this->assertEquals(1, $row['version']);
        $this->assertEquals($configurationData, $row['configuration']);
        $this->assertEquals('', $row['name']);
        $this->assertEquals('', $row['description']);
        $this->assertEquals(false, $row['isDisabled']);

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($configuration->getComponentId())
            ->setConfigurationId($configuration->getConfigurationId()));

        $this->assertCount(2, $rows);

        // copy to same second configuration
        $row = $components->createConfigurationRowFromVersion(
            $configuration->getComponentId(),
            $configuration->getConfigurationId(),
            101,
            1,
            $configuration2->getConfigurationId(), // use numeric id
        );

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($configuration->getComponentId())
            ->setConfigurationId($configuration->getConfigurationId()));

        $this->assertCount(2, $rows);

        $this->assertArrayHasKey('id', $row);
        $this->assertArrayHasKey('version', $row);
        $this->assertArrayHasKey('configuration', $row);
        $this->assertArrayHasKey('isDisabled', $row);
        $this->assertArrayHasKey('name', $row);
        $this->assertArrayHasKey('description', $row);

        $this->assertIsString($row['id']); // check id is string it is ULID
        $this->assertEquals(1, $row['version']);
        $this->assertEquals($configurationData, $row['configuration']);

        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($configuration2->getComponentId())
            ->setConfigurationId($configuration2->getConfigurationId()));

        $this->assertCount(1, $rows);
    }


    /**
     * @dataProvider provideComponentsClientType
     */
    public function testGetComponentConfigurations(): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);

        $configs = $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId('transformation'),
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
            (new ListComponentConfigurationsOptions())->setComponentId('transformation'),
        );
        $this->assertCount(2, $configs);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testGetComponentConfigurationsWithConfigAndRows(): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);

        $configs = $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId('transformation'),
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
            (new ListComponentConfigurationsOptions())->setComponentId('transformation'),
        );
        $this->assertCount(1, $configs);
        $this->assertEquals($configData1, $configs[0]['configuration']);
        $this->assertEquals($configData2, $configs[0]['rows'][0]['configuration']);
    }

    /**
     * Create configuration with few rows, update some row and then rollback to configuration with updated row
     *
     * @dataProvider provideComponentsClientType
     */
    public function testChangeDescription(): void
    {
        // test 1: create config
        $createChangeDescription = 'Create configuration';
        $componentConfig = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setConfiguration(['a' => 'b'])
            ->setName('Main')
            ->setChangeDescription($createChangeDescription);
        $components = new \Keboola\StorageApi\Components($this->client);
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
        $firstRowConfig = ['first' => 1];
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
        $firstRowUpdatedConfig = ['first' => 22];
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

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testConfigurationNameAndDescriptionShouldNotBeTrimmed(): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);
        $config = $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName("name\n")
            ->setDescription("description\n"));

        $this->assertEquals("name\n", $config['name']);
        $this->assertEquals("description\n", $config['description']);

        $config = $components->updateConfiguration((new  \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName("name2\n")
            ->setDescription("description2\n"));

        $this->assertEquals("name2\n", $config['name']);
        $this->assertEquals("description2\n", $config['description']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testConfigurationRowNameAndDescriptionShouldNotBeTrimmed(): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('name')
            ->setDescription('description');
        $components->addConfiguration($config);

        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $rowConfig->setName("name\n");
        $rowConfig->setDescription("description\n");
        $createdRow = $components->addConfigurationRow($rowConfig);
        $this->assertEquals("name\n", $createdRow['name']);
        $this->assertEquals("description\n", $createdRow['description']);

        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $rowConfig->setRowId($createdRow['id']);
        $rowConfig->setName("name2\n");
        $rowConfig->setDescription("description2\n");

        $updatedRow = $components->updateConfigurationRow($rowConfig);
        $this->assertEquals("name2\n", $updatedRow['name']);
        $this->assertEquals("description2\n", $updatedRow['description']);
    }

    /**
     * tests for https://github.com/keboola/connection/issues/977
     *
     * @dataProvider provideComponentsClientType
     */
    public function testRowChangesAfterConfigurationRollback(): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);

        // config version 1
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('name')
            ->setDescription('description');
        $components->addConfiguration($config);

        // config version 2
        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $createdRow = $components->addConfigurationRow($rowConfig);

        // config version 3
        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $rowConfig->setRowId($createdRow['id']);
        $rowConfig->setName('name');
        $rowConfig->setDescription('description');
        $rowConfig->setIsDisabled(true);
        $components->updateConfigurationRow($rowConfig);

        // rollback config version 2
        $components->rollbackConfiguration('wr-db', $config->getConfigurationId(), 2);
        $response = $components->getConfiguration('wr-db', $config->getConfigurationId());
        $this->assertEquals('', $response['rows'][0]['name']);
        $this->assertEquals('', $response['rows'][0]['description']);
        $this->assertEquals(false, $response['rows'][0]['isDisabled']);

        // rollback config version 3
        $components->rollbackConfiguration('wr-db', $config->getConfigurationId(), 3);
        $response = $components->getConfiguration('wr-db', $config->getConfigurationId());
        $this->assertEquals('name', $response['rows'][0]['name']);
        $this->assertEquals('description', $response['rows'][0]['description']);
        $this->assertEquals(true, $response['rows'][0]['isDisabled']);
    }

    /**
     * tests for https://github.com/keboola/connection/issues/977
     *
     * @dataProvider provideComponentsClientType
     */
    public function testRowChangesAfterConfigurationCopy(): void
    {
        $componentsApi = new \Keboola\StorageApi\Components($this->client);

        // config version 1
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('name')
            ->setDescription('description');
        $componentsApi->addConfiguration($config);

        // config version 2 - create row 1
        $rowConfig = new ConfigurationRow($config);
        $firstRow = $componentsApi->addConfigurationRow($rowConfig);

        // config version 3 - update row 1
        $rowConfig = new ConfigurationRow($config);
        $rowConfig->setRowId($firstRow['id']);
        $rowConfig->setName('first name');
        $rowConfig->setDescription('first description');
        $rowConfig->setIsDisabled(true);
        $componentsApi->updateConfigurationRow($rowConfig);

        // config version 4 - create row 2
        $rowConfig = new ConfigurationRow($config);
        $rowConfig->setName('second name');
        $rowConfig->setDescription('second description');
        $componentsApi->addConfigurationRow($rowConfig);

        // config version 5 - delete row 1
        $componentsApi->deleteConfigurationRow('wr-db', 'main-1', $firstRow['id']);

        // copy config version 2
        $copiedConfig = $componentsApi->createConfigurationFromVersion(
            'wr-db',
            $config->getConfigurationId(),
            2,
            'test',
        );
        $response = $componentsApi->getConfiguration('wr-db', $copiedConfig['id']);
        $this->assertSame('test', $response['name']);
        $this->assertSame('description', $response['description']);
        $this->assertSame('Copied from configuration "name" (main-1) version 2', $response['changeDescription']);
        // check rows
        $this->assertCount(1, $response['rows']);
        $this->assertEquals('', $response['rows'][0]['name']);
        $this->assertEquals('', $response['rows'][0]['description']);
        $this->assertEquals(
            'Copied from configuration "name" (main-1) version 2',
            $response['rows'][0]['changeDescription'],
        );
        $this->assertEquals(false, $response['rows'][0]['isDisabled']);

        // copy config version 4
        $copiedConfig = $componentsApi->createConfigurationFromVersion(
            'wr-db',
            $config->getConfigurationId(),
            4,
            'test',
            'some description',
            'some change description',
        );
        $response = $componentsApi->getConfiguration('wr-db', $copiedConfig['id']);
        $this->assertSame('test', $response['name']);
        $this->assertSame('some description', $response['description']);
        $this->assertSame('some change description', $response['changeDescription']);
        // check rows
        $this->assertCount(2, $response['rows']);

        $this->assertEquals('first name', $response['rows'][0]['name']);
        $this->assertEquals('first description', $response['rows'][0]['description']);
        $this->assertEquals(
            'Copied from configuration "name" (main-1) version 4',
            $response['rows'][0]['changeDescription'],
        );
        $this->assertEquals(true, $response['rows'][0]['isDisabled']);

        $this->assertEquals('second name', $response['rows'][1]['name']);
        $this->assertEquals('second description', $response['rows'][1]['description']);
        $this->assertEquals(
            'Copied from configuration "name" (main-1) version 4',
            $response['rows'][1]['changeDescription'],
        );
    }

    /**
     * tests for https://github.com/keboola/connection/issues/977
     *
     * @dataProvider provideComponentsClientType
     * @group global-search
     */
    public function testRowChangesAfterRowRollback(string $devBranchType): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);

        // config version 1
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('name')
            ->setDescription('description');
        $components->addConfiguration($config);

        $rowName = 'main-1-1'.$this->generateDescriptionForTestObject() . '-' . $devBranchType;
        $hashedRowName = sha1($rowName);
        // config version 2, row version 1
        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $rowConfig->setName($hashedRowName);
        $createdRow = $components->addConfigurationRow($rowConfig);

        $apiCall = fn() => $this->_client->globalSearch($hashedRowName);
        $assertCallback = function ($searchResult) use ($hashedRowName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration-row', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedRowName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        // config version 3, row version 2
        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $rowConfig->setRowId($createdRow['id']);
        $rowConfig->setName('name');
        $rowConfig->setDescription('description');
        $rowConfig->setIsDisabled(true);
        $components->updateConfigurationRow($rowConfig);

        $apiCall = fn() => $this->_client->globalSearch($hashedRowName);
        $assertCallback = function ($searchResult) {
            $this->assertSame(0, $searchResult['all'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        // rollback row version 1
        $components->rollbackConfigurationRow('wr-db', $config->getConfigurationId(), $createdRow['id'], 1);
        $response = $components->getConfiguration('wr-db', $config->getConfigurationId());
        $this->assertEquals($hashedRowName, $response['rows'][0]['name']);
        $this->assertEquals('', $response['rows'][0]['description']);
        $this->assertEquals(false, $response['rows'][0]['isDisabled']);

        $apiCall = fn() => $this->_client->globalSearch($hashedRowName);
        $assertCallback = function ($searchResult) use ($hashedRowName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('configuration-row', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedRowName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        // rollback row version 2
        $components->rollbackConfigurationRow('wr-db', $config->getConfigurationId(), $createdRow['id'], 2);
        $response = $components->getConfiguration('wr-db', $config->getConfigurationId());
        $this->assertEquals('name', $response['rows'][0]['name']);
        $this->assertEquals('description', $response['rows'][0]['description']);
        $this->assertEquals(true, $response['rows'][0]['isDisabled']);
    }

    /**
     * tests for https://github.com/keboola/connection/issues/977
     *
     * @dataProvider provideComponentsClientType
     */
    public function testRowChangesAfterRowCopy(): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);

        // config version 1
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('name')
            ->setDescription('description');
        $components->addConfiguration($config);

        // config version 2, row version 1
        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $rowConfig->setState(['rowStateKey' => 'rowStateValue']);
        $createdRow = $components->addConfigurationRow($rowConfig);

        // config version 3, row version 2
        $rowConfig = new \Keboola\StorageApi\Options\Components\ConfigurationRow($config);
        $rowConfig->setRowId($createdRow['id']);
        $rowConfig->setName('name');
        $rowConfig->setDescription('description');
        $rowConfig->setIsDisabled(true);
        $components->updateConfigurationRow($rowConfig);

        // copy row version 1
        $createdRow2 = $components->createConfigurationRowFromVersion('wr-db', $config->getConfigurationId(), $createdRow['id'], 1);
        $response = $components->getConfiguration('wr-db', $config->getConfigurationId());
        $this->assertStringMatchesFormat('Row %d copied from configuration "name" (main-1) row %d version 1', $response['changeDescription']);

        $row1 = $response['rows'][0];
        $this->assertEquals($createdRow['id'], $row1['id']);
        $this->assertEquals('name', $row1['name']);
        $this->assertEquals('description', $row1['description']);
        $this->assertEquals("Row {$createdRow["id"]} changed", $row1['changeDescription']);
        $this->assertEquals(true, $row1['isDisabled']);
        $this->assertNotEmpty($row1['state']);

        $row2 = $response['rows'][1];
        $this->assertEquals($createdRow2['id'], $row2['id']);
        $this->assertEquals('', $row2['name']);
        $this->assertEquals('', $row2['description']);
        $this->assertStringMatchesFormat('Copied from configuration "name" (main-1) row %d version 1', $row2['changeDescription']);
        $this->assertEquals(false, $row2['isDisabled']);
        $this->assertEmpty($row2['state']);

        // copy row version 2
        $createdRow3 = $components->createConfigurationRowFromVersion('wr-db', $config->getConfigurationId(), $createdRow['id'], 2);
        $response = $components->getConfiguration('wr-db', $config->getConfigurationId());
        $this->assertStringMatchesFormat('Row %d copied from configuration "name" (main-1) row %d version 2', $response['changeDescription']);

        $row1 = $response['rows'][0];
        $this->assertEquals($createdRow['id'], $row1['id']);
        $this->assertEquals('name', $row1['name']);
        $this->assertEquals('description', $row1['description']);
        $this->assertEquals("Row {$createdRow["id"]} changed", $row1['changeDescription']);
        $this->assertEquals(true, $row1['isDisabled']);
        $this->assertNotEmpty($row1['state']);

        $row2 = $response['rows'][1];
        $this->assertEquals($createdRow2['id'], $row2['id']);
        $this->assertEquals('', $row2['name']);
        $this->assertEquals('', $row2['description']);
        $this->assertStringMatchesFormat('Copied from configuration "name" (main-1) row %d version 1', $row2['changeDescription']);
        $this->assertEquals(false, $row2['isDisabled']);
        $this->assertEmpty($row2['state']);

        $row3 = $response['rows'][2];
        $this->assertEquals($createdRow3['id'], $row3['id']);
        $this->assertEquals('name', $row3['name']);
        $this->assertEquals('description', $row3['description']);
        $this->assertStringMatchesFormat('Copied from configuration "name" (main-1) row %d version 2', $row3['changeDescription']);
        $this->assertEquals(true, $row3['isDisabled']);
        $this->assertEmpty($row3['state']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testStateAttributeNotPresentInVersions(): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $this->assertArrayNotHasKey('state', $components->getConfigurationVersion('wr-db', 'main-1', 1));
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testRollbackPreservesState(): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $state = ['key' => 'val'];
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setState(['unknown' => 'undefined']);
        $components->addConfiguration($configuration);

        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Updated name');
        $components->updateConfiguration($configuration);

        $components->updateConfigurationState((new ConfigurationState())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setState($state));

        $components->rollbackConfiguration('wr-db', 'main-1', 1);

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(3, $configurationResponse['version']);
        $this->assertEquals($state, $configurationResponse['state']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testCopyResetsState(): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $state = ['key' => 'val'];
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setState(['unknown' => 'undefined']);
        $components->addConfiguration($configuration);

        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Updated name');
        $components->updateConfiguration($configuration);

        $components->updateConfigurationState((new ConfigurationState())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setState($state));

        $newConfig = $components->createConfigurationFromVersion('wr-db', 'main-1', 1, 'main-2');

        $configurationResponse = $components->getConfiguration('wr-db', $newConfig['id']);
        $this->assertEmpty($configurationResponse['state']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testRevertingConfigRowVersionWillNotCreateEmptyConfiguration(): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);
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
        $this->assertSame('Rollback to version 1 (via configuration rollback to version 2)', $rollbackedConfigRow['changeDescription']);
        $this->assertArrayEqualsExceptKeys($originalConfigRow, $rollbackedConfigRow, ['version', 'changeDescription']);
    }

    public function testCreateConfigurationWithEmptyStringIdWillGenerateTheId(): void
    {
        $components = new \Keboola\StorageApi\Components($this->client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $componentId = 'wr-db';
        $configurationId = '';
        $configuration
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main');

        $configurationResponse = $components->addConfiguration($configuration);

        $this->assertNotEmpty($configurationResponse['id']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testConfigurationStateUpdate(): void
    {
        $componentId = 'wr-db';
        $configurationId = 'main-1';

        $components = new \Keboola\StorageApi\Components($this->client);

        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setDescription('some desc'));

        $state = [
            'my' => 'test',
        ];

        $configState = (new \Keboola\StorageApi\Options\Components\ConfigurationState())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setState($state)
        ;

        $configuration = $components->updateConfigurationState($configState);

        $this->assertSame($state, $configuration['state']);
        $this->assertSame(1, $configuration['version']);
        $this->assertSame($configuration, $components->getConfiguration($componentId, $configurationId));

        $branchPrefix = '';
        if (!$this->client instanceof BranchAwareClient) {
            $branchPrefix = 'branch/default/';
        }
        $stateEndpoint = $branchPrefix . "components/{$componentId}/configs/{$configurationId}/state";

        try {
            $this->client->apiPutJson($stateEndpoint, [
                'state' => '{sdf}',
            ]);
            $this->fail('Post invalid json should not be allowed.');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.components.validation', $e->getStringCode());
            $this->assertEquals('Invalid parameters - state: This value should be valid JSON.', $e->getMessage());
        }

        try {
            $this->client->apiPutJson($stateEndpoint, [
                'description' => 'Test',
                'state' => json_encode('{}'),
            ]);
            $this->fail('Post additional fileds should not be allowed.');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.components.validation', $e->getStringCode());
            $this->assertEquals('Invalid parameters - description: This field was not expected.', $e->getMessage());
        }

        try {
            $this->client->apiPutJson($stateEndpoint, [
                'state' => '',
            ]);
            $this->fail('Post empty state should not be allowed.');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.components.validation', $e->getStringCode());
            $this->assertEquals('Invalid parameters - state: This value should not be blank.', $e->getMessage());
        }

        try {
            $this->client->apiPutJson($stateEndpoint, []);
            $this->fail('Post without state should not be allowed.');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.components.validation', $e->getStringCode());
            $this->assertEquals('Invalid parameters - state: This field is missing.', $e->getMessage());
        }

        $this->assertSame($configuration, $components->getConfiguration($componentId, $configurationId));
    }
}
