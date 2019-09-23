<?php

namespace Keboola\Test\Common;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Event;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\StorageApi\Options\TokenAbstractOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Options\TokenUpdateOptions;
use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Client;

class TokensTest extends StorageApiTestCase
{
    const BUCKET_PERMISSION_MANAGE = 'manage';

    /** @var string */
    private $inBucketId;

    /** @var string */
    private $outBucketId;

    public function setUp()
    {
        parent::setUp();

        $this->_initEmptyTestBuckets();

        $triggers = $this->_client->listTriggers();
        foreach ($triggers as $trigger) {
            $this->_client->deleteTrigger((int) $trigger['id']);
        }

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
        $this->assertTrue($currentToken['canPurgeTrash']);
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

    public function testKeenReadTokensRetrieve()
    {
        $this->expectException(\Keboola\StorageApi\ClientException::class);
        $this->expectExceptionMessage('Api endpoint \'storage/tokens/keen\' was removed from KBC');
        $this->_client->getKeenReadCredentials();
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
        $permission = TokenAbstractOptions::BUCKET_PERMISSION_READ;
        $options = (new TokenUpdateOptions($tokenId))
            ->addBucketPermission($this->outBucketId, $permission)
        ;

        $this->_client->updateToken($options);
        $token = $this->_client->getToken($tokenId);

        $bucketPermissions = $token['bucketPermissions'];
        $this->assertCount(1, $bucketPermissions);

        $this->assertEquals($this->outBucketId, key($bucketPermissions));
        $this->assertEquals($permission, reset($bucketPermissions));

        // read permissions
        $permission = TokenAbstractOptions::BUCKET_PERMISSION_WRITE;
        $options = (new TokenUpdateOptions($tokenId))
            ->addBucketPermission($this->outBucketId, $permission)
        ;

        $this->_client->updateToken($options);
        $token = $this->_client->getToken($tokenId);

        $bucketPermissions = $token['bucketPermissions'];
        $this->assertCount(1, $bucketPermissions);

        $this->assertEquals($this->outBucketId, key($bucketPermissions));
        $this->assertEquals($permission, reset($bucketPermissions));

        // invalid permission
        $options = (new TokenUpdateOptions($tokenId))
            ->addBucketPermission($this->outBucketId, self::BUCKET_PERMISSION_MANAGE)
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
            ->addBucketPermission($this->outBucketId, self::BUCKET_PERMISSION_MANAGE)
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
        $this->assertFalse($token['canPurgeTrash']);

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
            ->setCanPurgeTrash(true)
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
        $this->assertTrue($token['canPurgeTrash']);

        $this->assertEquals('My test token', $token['description']);

        $this->assertArrayHasKey('bucketPermissions', $token);

        $buckets = $this->_client->listBuckets();

        $bucketPermissions = $token['bucketPermissions'];
        $this->assertCount(count($buckets), $bucketPermissions);

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

    public function testTokenWithoutTokensManagePermissionCannotRefreshOtherTokens()
    {
        $options = (new TokenCreateOptions())
            ->setDescription('Out read token')
        ;

        $limitedAccessTokenId = $this->_client->createToken($options);
        $limitedAccessToken = $this->_client->getToken($limitedAccessTokenId);
        $limitAccessTokenClient = new \Keboola\StorageApi\Client([
            'token' => $limitedAccessToken['token'],
            'url' => STORAGE_API_URL
        ]);

        $otherTokenId = $this->_client->createToken($options);

        $this->expectException(ClientException::class);
        $this->expectExceptionCode(403);

        $limitAccessTokenClient->refreshToken($otherTokenId);
        $this->assertEquals($limitedAccessToken, $this->_client->getToken($limitedAccessTokenId));
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
            $components->getConfiguration($options->getComponentId(), $options->getConfigurationId());
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
        $configurationRow = new ConfigurationRow(
            (new Configuration())
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
        $options = (new TokenUpdateOptions($tokenId))
            ->addComponentAccess('provisioning')
        ;

        $this->_client->updateToken($options);

        $componentList = $components->listComponents();
        $this->assertCount(1, $componentList);

        $this->assertEquals('provisioning', $componentList[0]['id']);

        $this->clearComponents();
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
            ->addBucketPermission($this->outBucketId, TokenAbstractOptions::BUCKET_PERMISSION_READ)
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
            ->addBucketPermission($this->outBucketId, TokenAbstractOptions::BUCKET_PERMISSION_WRITE)
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
            ->addBucketPermission('out.non-existing', TokenAbstractOptions::BUCKET_PERMISSION_READ)
        ;

        try {
            $this->_client->createToken($options);
            $this->fail('Invalid permissions exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tokens.invalidPermissions', $e->getStringCode());
        }
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
            $this->assertEquals(self::BUCKET_PERMISSION_MANAGE, $permission);
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
            $this->assertEquals(self::BUCKET_PERMISSION_MANAGE, $permission);
        }

        $client->getBucket($newBucketId);
        $client->dropBucket($newBucketId);
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

    public function testTokenUpdateKeepsCanManageBucketsFlag()
    {
        $bucketIds = array_map(
            function ($bucket) {
                return $bucket['id'];
            },
            $this->_client->listBuckets()
        );

        $this->assertGreaterThan(0, count($bucketIds));

        $options = (new TokenCreateOptions())
            ->setDescription('CanManageBuckets')
            ->setCanManageBuckets(true)
        ;

        $tokenId = $this->_client->createToken($options);

        $token = $this->_client->getToken($tokenId);

        $this->assertTrue($token['canManageBuckets']);

        $this->assertCount(count($bucketIds), $token['bucketPermissions']);
        $this->assertEmpty(array_diff($bucketIds, array_keys($token['bucketPermissions'])));

        foreach ($token['bucketPermissions'] as $bucketPermission) {
            $this->assertSame(self::BUCKET_PERMISSION_MANAGE, $bucketPermission);
        }

        // update token and set buckets permissions
        $options = (new TokenUpdateOptions($tokenId))
            ->setDescription('CanManageBuckets update 1')
            ->addBucketPermission($this->outBucketId, TokenAbstractOptions::BUCKET_PERMISSION_READ)
        ;

        $this->_client->updateToken($options);

        $token = $this->_client->getToken($tokenId);

        $this->assertTrue($token['canManageBuckets']);

        $this->assertCount(count($bucketIds), $token['bucketPermissions']);
        $this->assertEmpty(array_diff($bucketIds, array_keys($token['bucketPermissions'])));

        foreach ($token['bucketPermissions'] as $bucketPermission) {
            $this->assertSame(self::BUCKET_PERMISSION_MANAGE, $bucketPermission);
        }

        // update token without setting permissions
        $options = (new TokenUpdateOptions($tokenId))
            ->setDescription('CanManageBuckets update 2')
        ;

        $this->_client->updateToken($options);

        $token = $this->_client->getToken($tokenId);

        $this->assertTrue($token['canManageBuckets']);

        $this->assertCount(count($bucketIds), $token['bucketPermissions']);
        $this->assertEmpty(array_diff($bucketIds, array_keys($token['bucketPermissions'])));

        foreach ($token['bucketPermissions'] as $bucketPermission) {
            $this->assertSame(self::BUCKET_PERMISSION_MANAGE, $bucketPermission);
        }
    }

    public function testTokenWithoutTokensManagePermissionCanListAndViewOnlySelf()
    {
        $initialTokens = $this->_client->listTokens();

        $options = (new TokenCreateOptions())
            ->setDescription('Token without canManageTokens permission')
        ;

        $tokenId = $this->_client->createToken($options);

        $tokens = $this->_client->listTokens();
        $this->assertCount(count($initialTokens) + 1, $tokens);

        $token = $this->_client->getToken($tokenId);
        $this->assertFalse($token['canManageTokens']);

        $client = new \Keboola\StorageApi\Client([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ]);

        $verifiedToken = $client->verifyToken();

        $tokens = $client->listTokens();
        $this->assertCount(1, $tokens);

        $token = reset($tokens);
        $this->assertSame($verifiedToken['id'], $token['id']);

        $token = $client->getToken($tokenId);
        $this->assertSame($verifiedToken['id'], $token['id']);

        $options = (new TokenCreateOptions())
            ->setDescription('Token without canManageTokens permission')
        ;

        $tokenId = $this->_client->createToken($options);

        try {
            $client->getToken($tokenId);
            $this->fail('Other token detail with no permissions');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }
    }

    public function testTokenWithoutCanPurgeTrashPermission()
    {
        $this->clearComponents();

        // create token without purge trash permission
        $options = (new TokenCreateOptions())
            ->setCanManageBuckets(true)
            ->setDescription('Token without canPurgeTrash permission')
        ;

        $tokenId = $this->_client->createToken($options);

        $token = $this->_client->getToken($tokenId);
        $this->assertFalse($token['canPurgeTrash']);

        $client = new \Keboola\StorageApi\Client([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ]);


        $components = new Components($client);

        $listOptions = (new ListComponentConfigurationsOptions())
            ->setComponentId('provisioning')
        ;

        $configurations = $components->listComponentConfigurations($listOptions);
        $this->assertCount(0, $configurations);

        // create test configuration
        $components->addConfiguration((new Configuration())
            ->setComponentId('provisioning')
            ->setConfigurationId('for-delete')
            ->setConfiguration(['foo' => 'bar'])
            ->setName('Main2'));

        $configurations = $components->listComponentConfigurations($listOptions);
        $this->assertCount(1, $configurations);

        // move configuration to trash
        $components->deleteConfiguration('provisioning', 'for-delete');

        $configurations = $components->listComponentConfigurations($listOptions);
        $this->assertCount(0, $configurations);

        $listDeletedOptions = (new ListComponentConfigurationsOptions())
            ->setComponentId('provisioning')
            ->setIsDeleted(true)
        ;

        $configurations = $components->listComponentConfigurations($listDeletedOptions);
        $this->assertCount(1, $configurations);

        try {
            $components->deleteConfiguration('provisioning', 'for-delete');
            $this->fail('Token without permission should not delete configuration from trash');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }

        $configurations = $components->listComponentConfigurations($listDeletedOptions);
        $this->assertCount(1, $configurations);

        // update token permission
        $options = (new TokenUpdateOptions($tokenId))
            ->setCanPurgeTrash(true)
        ;

        $this->_client->updateToken($options);

        $token = $this->_client->getToken($tokenId);
        $this->assertTrue($token['canPurgeTrash']);

        $components->deleteConfiguration('provisioning', 'for-delete');

        $configurations = $components->listComponentConfigurations($listDeletedOptions);
        $this->assertCount(0, $configurations);
    }

    public function testTokenEvents()
    {
        $options = (new TokenCreateOptions())
            ->setCanManageBuckets(true)
            ->setDescription('Token with canManageBuckets permission')
        ;

        $tokenId = $this->_client->createToken($options);

        $token = $this->_client->getToken($tokenId);

        $client = new \Keboola\StorageApi\Client([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ]);

        $event = new Event();
        $event->setComponent('dummy')
            ->setMessage($token['description'] . ' sample event');

        $event = $this->createAndWaitForEvent($event, $client);

        $tokenEvents = $this->_client->listTokenEvents($tokenId);
        $this->assertCount(2, $tokenEvents); // token created + sample event

        $this->assertSame($event, reset($tokenEvents));

        $events = $this->_client->listEvents();
        $this->assertGreaterThan(2, count($events));
    }

    public function testTokenWithoutTokensManagePermissionCanListOnlyOwnTokenEvents()
    {
        $options = (new TokenCreateOptions())
            ->setCanManageBuckets(true)
            ->setDescription('Token with canManageBuckets permission')
        ;

        $tokenId = $this->_client->createToken($options);
        $token = $this->_client->getToken($tokenId);

        $client = new \Keboola\StorageApi\Client([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ]);

        // check access for own events
        $event = new Event();
        $event->setComponent('dummy')
            ->setMessage($token['description'] . ' sample event');

        $event = $this->createAndWaitForEvent($event, $client);

        $tokenEvents = $client->listTokenEvents($tokenId);
        $this->assertCount(2, $tokenEvents); // token created + sample event

        $this->assertSame($event, reset($tokenEvents));

        $events = $this->_client->listEvents();
        $this->assertGreaterThan(2, count($events));

        // check access for other token events
        $verifiedToken = $this->_client->verifyToken();

        try {
            $client->listTokenEvents($verifiedToken['id']);
            $this->fail('Other token events with no permissions');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }
    }
}
