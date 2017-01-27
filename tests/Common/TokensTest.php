<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

namespace Keboola\Test\Common;

use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;

class TokensTest extends StorageApiTestCase
{

    protected $_inBucketId;
    protected $_outBucketId;

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();

        $this->_outBucketId = $this->getTestBucketId(self::STAGE_OUT);
        $this->_inBucketId = $this->getTestBucketId(self::STAGE_IN);

        $this->_initTokens();
    }

    protected function _initTokens()
    {
        foreach ($this->_client->listTokens() as $token) {
            if ($token['isMasterToken']) {
                continue;
            }
            $this->_client->dropToken($token['id']);
        }
    }

    protected function _clearComponents()
    {
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

    public function testTokenProperties()
    {
        $token = $this->_client->verifyToken();
        $this->arrayHasKey('created', $token);
        $this->arrayHasKey('description', $token);
        $this->arrayHasKey('id', $token);
        $this->assertTrue($token['isMasterToken']);
        $this->assertTrue($token['canManageBuckets']);
        $this->assertTrue($token['canReadAllFileUploads']);
        $this->assertFalse($token['isDisabled']);
        $this->assertNotEmpty($token['bucketPermissions']);
        $this->arrayHasKey('owner', $token);
        $this->arrayHasKey('admin', $token);

        $owner = $token['owner'];
        $this->assertInternalType('integer', $owner['dataSizeBytes']);
        $this->assertInternalType('integer', $owner['rowsCount']);
        $this->assertInternalType('boolean', $owner['hasRedshift']);

        $this->assertArrayHasKey('limits', $owner);
        $this->assertArrayHasKey('metrics', $owner);
        $this->assertArrayHasKey('defaultBackend', $owner);

        $firstLimit = reset($owner['limits']);
        $limitKeys = array_keys($owner['limits']);
        $this->assertArrayHasKey('name', $firstLimit);
        $this->assertArrayHasKey('value', $firstLimit);
        $this->assertInternalType('int', $firstLimit['value']);
        $this->assertEquals($firstLimit['name'], $limitKeys[0]);

        $tokens = $this->_client->listTokens();
        foreach ($tokens as $currentToken) {
            if ($currentToken['id'] != $token['id']) {
                continue;
            }

            $this->arrayHasKey($currentToken['admin']);

            $admin = $currentToken['admin'];
            $this->arrayHasKey('id', $admin);
            $this->arrayHasKey('name', $admin);
            $this->arrayHasKey('features', $admin);
            return;
        }

        $this->fail("Token $token[id] not present in list");
    }

    public function testKeenReadTokensRetrieve()
    {
        $keen = $this->_client->getKeenReadCredentials();
        $this->assertArrayHasKey('keenToken', $keen);
        $this->assertNotEmpty($keen['keenToken']);
        $this->assertNotEmpty($keen['projectId']);
    }

    public function testInvalidToken()
    {
        $invalidToken = 'tohlejeneplatnytoken';

        try {
            $client = new \Keboola\StorageApi\Client(array(
                'token' => $invalidToken,
                'url' => STORAGE_API_URL,
            ));
            $client->verifyToken();
            $this->fail('Exception should be thrown on invalid token');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertNotContains($invalidToken, $e->getMessage(), "Token value should not be returned back");
        }
    }

    public function testTokenManagement()
    {
        $initialTokens = $this->_client->listTokens();
        $description = 'Out read token';
        $bucketPermissions = array(
            $this->_outBucketId => 'read',
        );

        $tokenId = $this->_client->createToken($bucketPermissions, $description);

        // check created token
        $token = $this->_client->getToken($tokenId);
        $this->assertEquals($description, $token['description']);
        $this->assertFalse($token['canManageTokens']);
        $this->assertFalse($token['canManageBuckets']);
        $this->assertEquals($bucketPermissions, $token['bucketPermissions']);

        $currentToken = $this->_client->verifyToken();
        $this->assertArrayHasKey('creatorToken', $token);

        $creatorToken = $token['creatorToken'];
        $this->assertEquals($currentToken['id'], $creatorToken['id']);
        $this->assertEquals($currentToken['description'], $creatorToken['description']);

        $tokens = $this->_client->listTokens();
        $this->assertCount(count($initialTokens) + 1, $tokens);

        // update and check token again
        $newBucketPermissions = array(
            $this->_inBucketId => 'write',
        );
        $this->_client->updateToken($tokenId, $newBucketPermissions);
        $token = $this->_client->getToken($tokenId);
        $this->assertEquals($newBucketPermissions, $token['bucketPermissions']);

        // invalid permission
        $invalidBucketPermissions = array(
            $this->_inBucketId => 'manage',
        );
        try {
            $this->_client->updateToken($tokenId, $invalidBucketPermissions);
            $this->fail('Manage permissions shouild not be allower to set');
        } catch (\Keboola\StorageApi\ClientException $e) {
        }
        $token = $this->_client->getToken($tokenId);
        $this->assertEquals($newBucketPermissions, $token['bucketPermissions']);

        // drop token test
        $this->_client->dropToken($tokenId);
        $tokens = $this->_client->listTokens();
        $this->assertCount(count($initialTokens), $tokens);
    }

    public function testCreateTokenWithoutDescription()
    {
        $currentToken = $this->_client->verifyToken();
        $newTokenId = $this->_client->createToken(array());
        $newToken = $this->_client->getToken($newTokenId);

        $this->assertEquals('Created by ' . $currentToken['description'], $newToken['description']);

        $this->_client->dropToken($newTokenId);
    }

    public function testTokenRefresh()
    {
        $description = 'Out read token';
        $bucketPermissions = array(
            $this->_inBucketId => 'read'
        );
        $tokenId = $this->_client->createToken($bucketPermissions, $description);
        $token = $this->_client->getToken($tokenId);

        $this->_client->refreshToken($tokenId);
        $tokenAfterRefresh = $this->_client->getToken($tokenId);

        $this->assertNotEquals($token['token'], $tokenAfterRefresh['token']);
    }


    public function testTokenComponentAccess()
    {

        $this->_clearComponents();

        $description = "Component Access Test Token";
        $componentAccess = array("wr-db");
        $bucketPermissions = array($this->_inBucketId => "write");

        $componentAccessTokenId = $this->_client->createToken($bucketPermissions, $description, null, false, $componentAccess);
        $componentAccessToken = $this->_client->getToken($componentAccessTokenId);

        $componentFailTokenId = $this->_client->createToken($bucketPermissions, $description);
        $componentFailToken = $this->_client->getToken($componentFailTokenId);

        $accessClient = new \Keboola\StorageApi\Client(array(
            'token' => $componentAccessToken['token'],
            'url' => STORAGE_API_URL
        ));

        $failClient = new \Keboola\StorageApi\Client(array(
            'token' => $componentFailToken['token'],
            'url' => STORAGE_API_URL
        ));

        // we'll test 3 scenarios, using an admin token, a token with some component access, and a token with no access
        $adminComponentClient = new \Keboola\StorageApi\Components($this->_client);
        $accessComponentClient = new \Keboola\StorageApi\Components($accessClient);
        $failComponentClient = new \Keboola\StorageApi\Components($failClient);

        // we should have no components at the start
        $allComponents = $adminComponentClient->listComponents();
        $this->assertCount(0, $allComponents);

        // we'll create some initial test configuration
        $adminComponentClient->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main1'));
        $adminComponentClient->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-2')
            ->setConfiguration(array('x' => 'y'))
            ->setName('Main2'));
        $provisioningConfig = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('provisioning')
            ->setConfigurationId('main-1')
            ->setName('Main1');
        $adminComponentClient->addConfiguration($provisioningConfig);

        // make sure our admin token sees all
        $allComponents = $adminComponentClient->listComponents();
        $this->assertCount(2, $allComponents);

        $accessibleComponents = $accessComponentClient->listComponents();
        $this->assertCount(1, $accessibleComponents);
        $this->assertEquals($componentAccess[0], $accessibleComponents[0]['id']);

        try {
            $failComponents = $failComponentClient->listComponents();
            $this->fail("This token should not be allowed to access components API");
        } catch (\Keboola\StorageApi\ClientException  $e) {
        }

        // we should be able to read a config from our accessible component
        $config = $accessComponentClient->getConfiguration("wr-db", "main-1");
        $this->assertEquals($config["name"], "Main1");

        // we should be able to add a configuration for our accessible component
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-3')
            ->setName('Main3');
        $accessComponentClient->addConfiguration($config);

        // check that we can update our configuration
        $newName = "MAIN-3";
        $newConfigData = array("foo" => "bar");
        $config->setName($newName);
        $config->setConfiguration($newConfigData);
        $accessComponentClient->updateConfiguration($config);
        $updatedConfig = $accessComponentClient->getConfiguration($config->getComponentId(), $config->getConfigurationId());
        $this->assertEquals($updatedConfig['name'], $newName);
        $this->assertEquals($updatedConfig['configuration'], $newConfigData);

        // we should be able to delete this configuration too
        $accessComponentClient->deleteConfiguration($config->getComponentId(), $config->getConfigurationId());
        try {
            // it should be gone now, and throw a 404
            $deletedConfig = $accessComponentClient->getConfiguration($config->getComponentId(), $config->getConfigurationId());
            $this->fail("Configuration should no longer exist, throw a 404");
        } catch (\Keboola\StorageApi\ClientException  $e) {
            $this->assertEquals(404, $e->getCode());
        }

        // we should not be able to add a configuration for any other components
        try {
            $accessComponentClient->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
                ->setComponentId('provisioning')
                ->setConfigurationId('main-2')
                ->setConfiguration(array('foo' => 'bar'))
                ->setName('Main2'));
            $this->fail("Token was not granted access to this component, should throw an exception");
        } catch (\Keboola\StorageApi\ClientException  $e) {
        }

        // tokens should not be able to add configuration rows to configs of components that are inaccessible
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($provisioningConfig);
        $configurationRow->setRowId('main-1-1');
        try {
            $accessComponentClient->addConfigurationRow($configurationRow);
            $this->fail("Token was not granted access to this component, should throw an exception");
        } catch (\Keboola\StorageApi\ClientException  $e) {
        }

        // the token with no component access should not be able to add configurations
        try {
            $failComponentClient->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
                ->setComponentId('provisioning')
                ->setConfigurationId('main-2')
                ->setConfiguration(array('foo' => 'bar'))
                ->setName('Main2'));
            $this->fail("Token was not granted access to this component, should throw an exception");
        } catch (\Keboola\StorageApi\ClientException  $e) {
        }

        // let's grant the fail token access to the provisioning component
        $newFailTokenId = $this->_client->updateToken($componentFailTokenId, $bucketPermissions, $description, null, ["provisioning"]);
        $this->assertEquals($newFailTokenId, $componentFailTokenId);
        // the fail client should be able to see this component now
        $nonFailComponents = $failComponentClient->listComponents();
        $this->assertCount(1, $nonFailComponents);
        $this->assertEquals("provisioning", $nonFailComponents[0]["id"]);

        // cleanup
        $this->_clearComponents();
    }

    public function testTokenPermissions()
    {
        // prepare token and test tables
        $inTableId = $this->_client->createTable($this->_inBucketId, 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));
        $outTableId = $this->_client->createTable($this->_outBucketId, 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));

        $description = 'Out read token';
        $bucketPermissions = array(
            $this->_outBucketId => 'read'
        );
        $tokenId = $this->_client->createToken($bucketPermissions, $description);
        $token = $this->_client->getToken($tokenId);

        $client = new \Keboola\StorageApi\Client(array(
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ));

        // token getter
        $this->assertEquals($client->getTokenString(), $token['token']);
        $this->assertEmpty($token['expires']);
        $this->assertFalse($token['isExpired']);

        // check assigned buckets
        $buckets = $client->listBuckets();
        $this->assertCount(1, $buckets);
        $bucket = reset($buckets);
        $this->assertEquals($this->_outBucketId, $bucket['id']);

        // check assigned tables
        $tables = $client->listTables();
        $this->assertCount(1, $tables);
        $table = reset($tables);
        $this->assertEquals($outTableId, $table['id']);

        // read from table
        $tableData = $client->exportTable($outTableId);
        $this->assertNotEmpty($tableData);

        try {
            $client->exportTable($inTableId);
            $this->fail('Table exported with no permissions');
        } catch (\Keboola\StorageApi\ClientException $e) {
        }

        // write into table
        try {
            $client->writeTable($outTableId, new CsvFile(__DIR__ . '/../_data/languages.csv'));
            $this->fail('Table imported with read token');
        } catch (\Keboola\StorageApi\ClientException  $e) {
        }

        // table attribute
        try {
            $client->setTableAttribute($outTableId, 'my', 'value');
            $this->fail('Table attribute written with read token');
        } catch (\Keboola\StorageApi\ClientException $e) {
        }

        try {
            $client->writeTable($inTableId, new CsvFile(__DIR__ . '/../_data/languages.csv'));
            $this->fail('Table imported with no permissions');
        } catch (\Keboola\StorageApi\ClientException  $e) {
        }
    }

    public function testAssignNonExistingBucketShouldFail()
    {
        $bucketPermissions = array(
            'out.tohle-je-hodne-blby-nazev' => 'read'
        );

        try {
            $this->_client->createToken($bucketPermissions, 'Some description');
            $this->fail('Invalid permissions exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tokens.invalidPermissions', $e->getStringCode());
        }
    }

    public function testAllBucketsTokenPermissions()
    {
        $description = 'Out read token';
        $bucketsInitialCount = count($this->_client->listBuckets());
        $tokenId = $this->_client->createToken('manage', $description);
        $token = $this->_client->getToken($tokenId);

        $client = new \Keboola\StorageApi\Client(array(
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ));

        // token getter
        $this->assertEquals($client->getTokenString(), $token['token']);
        $this->assertEmpty($token['expires']);
        $this->assertFalse($token['isExpired']);

        // check assigned buckets
        $buckets = $client->listBuckets();
        $this->assertCount($bucketsInitialCount, $buckets);

        // create new bucket with master token
        $newBucketId = $this->_client->createBucket('test', 'in', 'testing');

        // check if new token has access to token
        $buckets = $client->listBuckets();
        $this->assertCount($bucketsInitialCount + 1, $buckets);

        $bucket = $client->getBucket($newBucketId);
        $client->dropBucket($newBucketId);
    }

    public function testTokenWithExpiration()
    {
        $description = 'Out read token with expiration';
        $bucketPermissions = array(
            $this->_outBucketId => 'read'
        );
        $twoMinutesExpiration = 2 * 60;
        $tokenId = $this->_client->createToken($bucketPermissions, $description, $twoMinutesExpiration);
        $token = $this->_client->getToken($tokenId);

        $client = new \Keboola\StorageApi\Client(array(
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ));
        $token = $client->verifyToken();
        $this->assertNotEmpty($token['expires']);
        $this->assertFalse($token['isExpired']);
    }

    public function testExpiredToken()
    {
        $description = 'Out read token with expiration';
        $bucketPermissions = array(
            $this->_outBucketId => 'read'
        );
        $oneSecondExpiration = 1;
        $tokenId = $this->_client->createToken($bucketPermissions, $description, $oneSecondExpiration);
        $token = $this->_client->getToken($tokenId);
        $tries = 0;

        $client = null;
        try {
            while ($tries < 7) {
                $client = new \Keboola\StorageApi\Client(array(
                    'token' => $token['token'],
                    'url' => STORAGE_API_URL,
                ));
                $client->verifyToken();
                sleep(pow(2, $tries++));
            }
            $this->fail('token should be invalid');
        } catch (\Keboola\StorageApi\ClientException $e) {
            if ($e->getStringCode() !== 'storage.tokenExpired') {
                $this->fail('storage.tokenExpired code should be rerturned from API.');
            }
        }
    }
}
