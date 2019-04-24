<?php
namespace Keboola\Test\Common;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Options\TokenUpdateOptions;
use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Client;

class TokensTest extends StorageApiTestCase
{
    /** @var string */
    private $inBucketId;

    /** @var string */
    private $outBucketId;

    public function setUp()
    {
        parent::setUp();

        $this->_initEmptyTestBuckets();

        $this->outBucketId = $this->getTestBucketId(self::STAGE_OUT);
        $this->inBucketId = $this->getTestBucketId(self::STAGE_IN);

        $this->initTokens();
    }

    private function initTokens()
    {
        foreach ($this->_client->listTokens() as $token) {
            if ($token['isMasterToken']) {
                continue;
            }

            $this->_client->dropToken($token['id']);
        }
    }

    private function clearComponents()
    {
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

    private function initTestConfigurations()
    {
        $components = new Components($this->_client);

        $this->assertCount(0, $components->listComponents());

        $components->addConfiguration((new Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main1'));

        $components->addConfiguration((new Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-2')
            ->setConfiguration(array('x' => 'y'))
            ->setName('Main2'));

        $components->addConfiguration((new Configuration())
            ->setComponentId('provisioning')
            ->setConfigurationId('main-1')
            ->setName('Main1'));
    }

    public function testTokenRefresh()
    {
        $options = (new TokenCreateOptions())
            ->setDescription('Out read token')
        ;

        $tokenId = $this->_client->createToken($options);
        $token = $this->_client->getToken($tokenId);

        $tokenString = $token['token'];
        $created = new \DateTime($token['created']);

        sleep(1);

        $this->_client->refreshToken($tokenId);
        $token = $this->_client->getToken($tokenId);

        $refreshed = new \DateTime($token['refreshed']);

        $this->assertNotEquals($tokenString, $token['token']);
        $this->assertGreaterThan($created->getTimestamp(), $refreshed->getTimestamp());
    }

    public function testTokenWithExpiration()
    {
        $options = (new TokenCreateOptions())
            ->setDescription('Out read token with expiration')
            ->setExpiresIn(2 * 60)
        ;

        $tokenId = $this->_client->createToken($options);
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
        $options = (new TokenCreateOptions())
            ->setDescription('Out read token with expiration')
            ->setExpiresIn(1)
        ;

        $tokenId = $this->_client->createToken($options);
        $token = $this->_client->getToken($tokenId);
        $tries = 0;

        $this->expectException(ClientException::class);
        while ($tries < 7) {
            $client = new Client([
                'token' => $token['token'],
                'url' => STORAGE_API_URL,
            ]);
            $client->verifyToken();
            sleep(pow(2, $tries++));
        }

        $this->fail('token should be invalid');
    }

    public function testAllBucketsTokenPermissions()
    {
        $bucketsInitialCount = count($this->_client->listBuckets());

        $options = (new TokenCreateOptions())
            ->setDescription('Out buckets manage token')
            ->setCanManageBuckets(true)
        ;

        $tokenId = $this->_client->createToken($options);
        $token = $this->_client->getToken($tokenId);

        $client = new \Keboola\StorageApi\Client(array(
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ));

        // token getter
        $this->assertEquals($client->getTokenString(), $token['token']);
        $this->assertEmpty($token['expires']);
        $this->assertFalse($token['isExpired']);

        $this->assertCount($bucketsInitialCount, $token['bucketPermissions']);
        foreach ($token['bucketPermissions'] as $bucketId => $permission) {
            $this->assertEquals(TokenUpdateOptions::ALL_BUCKETS_PERMISSION, $permission);
        }

        // check assigned buckets
        $buckets = $client->listBuckets();
        $this->assertCount($bucketsInitialCount, $buckets);

        // create new bucket with master token
        $newBucketId = $this->_client->createBucket('test', 'in', 'testing');

        // check if new token has access to token
        $buckets = $client->listBuckets();
        $this->assertCount($bucketsInitialCount + 1, $buckets);

        $token = $this->_client->getToken($tokenId);
        $this->assertCount($bucketsInitialCount + 1, $token['bucketPermissions']);
        foreach ($token['bucketPermissions'] as $bucketId => $permission) {
            $this->assertEquals(TokenUpdateOptions::ALL_BUCKETS_PERMISSION, $permission);
        }

        $client->getBucket($newBucketId);
        $client->dropBucket($newBucketId);
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
        $invalidToken = 'thisIsInvalidToken';

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

    public function testTokenProperties()
    {
        $currentToken = $this->_client->verifyToken();

        $this->assertArrayHasKey('created', $currentToken);
        $this->assertArrayHasKey('refreshed', $currentToken);
        $this->assertArrayHasKey('description', $currentToken);
        $this->assertArrayHasKey('id', $currentToken);

        $this->assertTrue($currentToken['isMasterToken']);
        $this->assertTrue($currentToken['canManageBuckets']);
        $this->assertTrue($currentToken['canReadAllFileUploads']);
        $this->assertFalse($currentToken['isDisabled']);
        $this->assertNotEmpty($currentToken['bucketPermissions']);
        $this->assertArrayHasKey('owner', $currentToken);
        $this->assertArrayHasKey('admin', $currentToken);

        $owner = $currentToken['owner'];
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

        $tokenFound = false;
        foreach ($this->_client->listTokens() as $token) {
            if ($token['id'] !== $currentToken['id']) {
                continue;
            }

            $this->assertArrayHasKey('admin', $token);

            $admin = $token['admin'];
            $this->assertArrayHasKey('id', $admin);
            $this->assertArrayHasKey('name', $admin);

            $tokenFound = true;
        }

        $this->assertTrue($tokenFound);
    }

    public function testBucketReadTokenPermission()
    {
        $outTableId = $this->_client->createTable(
            $this->outBucketId,
            'languages',
            new CsvFile(__DIR__ . '/../_data/languages.csv')
        );

        $options = (new TokenCreateOptions())
            ->setDescription('Out read token')
            ->addBucketPermission($this->outBucketId, TokenUpdateOptions::BUCKET_PERMISSION_READ)
        ;

        $tokenId = $this->_client->createToken($options);
        $token = $this->_client->getToken($tokenId);

        $client = new \Keboola\StorageApi\Client(array(
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ));

        // check assigned buckets
        $buckets = $client->listBuckets();
        $this->assertCount(1, $buckets);

        $bucket = reset($buckets);
        $this->assertEquals($this->outBucketId, $bucket['id']);

        // check assigned tables
        $tables = $client->listTables();
        $this->assertCount(1, $tables);

        $table = reset($tables);
        $this->assertEquals($outTableId, $table['id']);

        // read from table
        $tableData = $client->getTableDataPreview($outTableId);
        $this->assertNotEmpty($tableData);

        // write into table
        try {
            $client->writeTable($outTableId, new CsvFile(__DIR__ . '/../_data/languages.csv'));
            $this->fail('Table imported with read token');
        } catch (ClientException  $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        // table attribute
        try {
            $client->setTableAttribute($outTableId, 'my', 'value');
            $this->fail('Table attribute written with read token');
        } catch (ClientException $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }
    }

    public function testBucketWriteTokenPermission()
    {
        $outTableId = $this->_client->createTable(
            $this->outBucketId,
            'languages',
            new CsvFile(__DIR__ . '/../_data/languages.csv')
        );

        $options = (new TokenCreateOptions())
            ->setDescription('Out write token')
            ->addBucketPermission($this->outBucketId, TokenUpdateOptions::BUCKET_PERMISSION_WRITE)
        ;

        $tokenId = $this->_client->createToken($options);
        $token = $this->_client->getToken($tokenId);

        $client = new \Keboola\StorageApi\Client(array(
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ));

        // check assigned buckets
        $buckets = $client->listBuckets();
        $this->assertCount(1, $buckets);

        $bucket = reset($buckets);
        $this->assertEquals($this->outBucketId, $bucket['id']);

        // check assigned tables
        $tables = $client->listTables();
        $this->assertCount(1, $tables);

        $table = reset($tables);
        $this->assertEquals($outTableId, $table['id']);

        // read from table
        $tableData = $client->getTableDataPreview($outTableId);
        $this->assertNotEmpty($tableData);

        // write into table
        $client->writeTable($outTableId, new CsvFile(__DIR__ . '/../_data/languages.csv'));

        // table attribute
        $client->setTableAttribute($outTableId, 'my', 'value');
    }

    public function testNoBucketTokenPermission()
    {
        $outTableId = $this->_client->createTable(
            $this->outBucketId,
            'languages',
            new CsvFile(__DIR__ . '/../_data/languages.csv')
        );

        $options = (new TokenCreateOptions())
            ->setDescription('No bucket permission token')
        ;

        $tokenId = $this->_client->createToken($options);
        $token = $this->_client->getToken($tokenId);

        $client = new \Keboola\StorageApi\Client(array(
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ));

        // check assigned buckets
        $buckets = $client->listBuckets();
        $this->assertCount(0, $buckets);

        // check assigned tables
        $tables = $client->listTables();
        $this->assertCount(0, $tables);

        // read from table
        try {
            $client->getTableDataPreview($outTableId);
            $this->fail('Table exported with no permission token');
        } catch (ClientException  $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        // write into table
        try {
            $client->writeTable($outTableId, new CsvFile(__DIR__ . '/../_data/languages.csv'));
            $this->fail('Table imported with no permission token');
        } catch (ClientException  $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        // table attribute
        try {
            $client->setTableAttribute($outTableId, 'my', 'value');
            $this->fail('Table attribute with no permission token');
        } catch (ClientException $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }
    }

    public function testAssignNonExistingBucketShouldFail()
    {
        $options = (new TokenCreateOptions())
            ->setDescription('Some description')
            ->addBucketPermission('out.non-existing', TokenUpdateOptions::BUCKET_PERMISSION_READ)
        ;

        try {
            $this->_client->createToken($options);
            $this->fail('Invalid permissions exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tokens.invalidPermissions', $e->getStringCode());
        }
    }

    public function testBucketPermissionUpdate()
    {
        $options = (new TokenCreateOptions())
            ->setDescription('Some description')
        ;

        $tokenId = $this->_client->createToken($options);
        $token = $this->_client->getToken($tokenId);

        $bucketPermissions = $token['bucketPermissions'];
        $this->assertCount(0, $bucketPermissions);

        // read permissions
        $permission = TokenUpdateOptions::BUCKET_PERMISSION_READ;
        $options = (new TokenUpdateOptions())
            ->addBucketPermission($this->outBucketId, $permission)
            ->setTokenId($tokenId)
        ;

        $this->_client->updateToken($options);
        $token = $this->_client->getToken($tokenId);

        $bucketPermissions = $token['bucketPermissions'];
        $this->assertCount(1, $bucketPermissions);

        $this->assertEquals($this->outBucketId, key($bucketPermissions));
        $this->assertEquals($permission, reset($bucketPermissions));

        // read permissions
        $permission = TokenUpdateOptions::BUCKET_PERMISSION_WRITE;
        $options = (new TokenUpdateOptions())
            ->addBucketPermission($this->outBucketId, $permission)
            ->setTokenId($tokenId)
        ;

        $this->_client->updateToken($options);
        $token = $this->_client->getToken($tokenId);

        $bucketPermissions = $token['bucketPermissions'];
        $this->assertCount(1, $bucketPermissions);

        $this->assertEquals($this->outBucketId, key($bucketPermissions));
        $this->assertEquals($permission, reset($bucketPermissions));

        // invalid permission
        $options = (new TokenUpdateOptions())
            ->addBucketPermission($this->outBucketId, TokenUpdateOptions::ALL_BUCKETS_PERMISSION)
            ->setTokenId($tokenId)
        ;

        try {
            $this->_client->updateToken($options);
            $this->fail('Manage permissions should not be allowed to set');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tokens.invalidPermissions', $e->getStringCode());
        }

        $token = $this->_client->getToken($tokenId);

        $bucketPermissions = $token['bucketPermissions'];
        $this->assertCount(1, $bucketPermissions);

        $this->assertEquals($this->outBucketId, key($bucketPermissions));
        $this->assertEquals($permission, reset($bucketPermissions));
    }

    public function testAssignNonExistingBucketPermissionShouldFail()
    {
        $options = (new TokenCreateOptions())
            ->setDescription('Some description')
            ->addBucketPermission($this->outBucketId, TokenUpdateOptions::ALL_BUCKETS_PERMISSION)
        ;

        try {
            $this->_client->createToken($options);
            $this->fail('Invalid permissions exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tokens.invalidPermissions', $e->getStringCode());
        }
    }

    public function testTokenDrop()
    {
        $initialTokens = $this->_client->listTokens();

        $tokenId = $this->_client->createToken(new TokenCreateOptions());

        $tokens = $this->_client->listTokens();
        $this->assertCount(count($initialTokens) + 1, $tokens);

        $this->_client->dropToken($tokenId);

        $tokens = $this->_client->listTokens();
        $this->assertCount(count($initialTokens), $tokens);
    }

    public function testTokenDefaultOptions()
    {
        $currentToken = $this->_client->verifyToken();

        $tokenId = $this->_client->createToken(new TokenCreateOptions());
        $token = $this->_client->getToken($tokenId);

        $this->assertNull($token['expires']);

        $this->assertFalse($token['isMasterToken']);
        $this->assertFalse($token['canManageBuckets']);
        $this->assertFalse($token['canManageTokens']);
        $this->assertFalse($token['canReadAllFileUploads']);

        $this->assertEquals('Created by ' . $currentToken['description'], $token['description']);

        $this->assertArrayHasKey('bucketPermissions', $token);
        $this->assertCount(0, $token['bucketPermissions']);

        $this->assertArrayNotHasKey('admin', $token);
        $this->assertArrayNotHasKey('componentAccess', $token);

        $this->assertArrayHasKey('creatorToken', $token);

        $creator = $token['creatorToken'];
        $this->assertEquals($currentToken['id'], $creator['id']);
        $this->assertEquals($currentToken['description'], $creator['description']);
    }

    public function testTokenMaximalOptions()
    {
        $currentToken = $this->_client->verifyToken();

        $options = (new TokenCreateOptions())
            ->setDescription('My test token')
            ->setCanReadAllFileUploads(true)
            ->setCanManageBuckets(true)
            ->setExpiresIn(360)
            ->addComponentAccess('wr-db')
        ;

        $tokenId = $this->_client->createToken($options);
        $token = $this->_client->getToken($tokenId);

        $this->assertNotNull($token['expires']);

        $this->assertFalse($token['isMasterToken']);
        $this->assertFalse($token['canManageTokens']);

        $this->assertTrue($token['canManageBuckets']);
        $this->assertTrue($token['canReadAllFileUploads']);

        $this->assertEquals('My test token', $token['description']);

        $this->assertArrayHasKey('bucketPermissions', $token);

        $bucketPermissions = $token['bucketPermissions'];
        $this->assertCount(2, $bucketPermissions);

        $this->assertArrayNotHasKey('admin', $token);

        $this->assertArrayHasKey('creatorToken', $token);

        $creator = $token['creatorToken'];
        $this->assertEquals($currentToken['id'], $creator['id']);
        $this->assertEquals($currentToken['description'], $creator['description']);

        $this->assertArrayHasKey('componentAccess', $token);

        $componentAccess = $token['componentAccess'];
        $this->assertCount(1, $componentAccess);

        $this->assertEquals('wr-db', reset($componentAccess));
    }

    public function testTokenComponentAccess()
    {
        $this->clearComponents();
        $this->initTestConfigurations();

        $options = (new TokenCreateOptions())
            ->setDescription('Component Access Test Token')
            ->addComponentAccess('wr-db')
        ;

        $tokenId = $this->_client->createToken($options);
        $token = $this->_client->getToken($tokenId);

        $client = new \Keboola\StorageApi\Client([
            'token' => $token['token'],
            'url' => STORAGE_API_URL
        ]);

        $components = new Components($client);

        $config = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals($config['name'], 'Main1');

        // we should be able to add a configuration for our accessible component
        $options = (new Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-3')
            ->setName('Main3')
        ;

        $components->addConfiguration($options);

        // check that we can update our configuration
        $newName = 'MAIN-3';
        $newConfigData = ['foo' => 'bar'];

        $options->setName($newName);
        $options->setConfiguration($newConfigData);

        $components->updateConfiguration($options);

        $updatedConfig = $components->getConfiguration($options->getComponentId(), $options->getConfigurationId());
        $this->assertEquals($updatedConfig['name'], $newName);
        $this->assertEquals($updatedConfig['configuration'], $newConfigData);


        // we should be able to delete this configuration too
        $components->deleteConfiguration($options->getComponentId(), $options->getConfigurationId());

        try {
            // it should be gone now, and throw a 404
            $deletedConfig = $components->getConfiguration($options->getComponentId(), $options->getConfigurationId());
            $this->fail('Configuration should no longer exist, throw a 404');
        } catch (ClientException $e) {
            $this->assertEquals(404, $e->getCode());
        }

        $this->clearComponents();
    }

    public function testTokenComponentAccessError()
    {
        $this->clearComponents();
        $this->initTestConfigurations();

        $options = (new TokenCreateOptions())
            ->setDescription('Component Access Test Token')
        ;

        $tokenId = $this->_client->createToken($options);
        $token = $this->_client->getToken($tokenId);

        $client = new \Keboola\StorageApi\Client([
            'token' => $token['token'],
            'url' => STORAGE_API_URL
        ]);

        $components = new Components($client);

        try {
            $components->listComponents();
            $this->fail("This token should not be allowed to access components API");
        } catch (ClientException $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        // the token with no component access should not be able to add configurations
        try {
            $components->addConfiguration((new Configuration())
                ->setComponentId('provisioning')
                ->setConfigurationId('main-2')
                ->setConfiguration(['foo' => 'bar'])
                ->setName('Main2'));
            $this->fail("Token was not granted access to this component, should throw an exception");
        } catch (ClientException $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        // the token with no component access should not be able to add configuration rows
        $configurationRow = new ConfigurationRow((new Configuration())
            ->setComponentId('provisioning')
            ->setConfigurationId('main-1')
        );

        $configurationRow->setRowId('main-1-1');

        try {
            $components->addConfigurationRow($configurationRow);
            $this->fail("Token was not granted access to this component, should throw an exception");
        } catch (ClientException  $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        // grant permission to component
        $options = (new TokenUpdateOptions())
            ->addComponentAccess('provisioning')
            ->setTokenId($tokenId)
        ;

        $this->_client->updateToken($options);

        $componentList = $components->listComponents();
        $this->assertCount(1, $componentList);

        $this->assertEquals('provisioning', $componentList[0]['id']);

        $this->clearComponents();
    }
}
