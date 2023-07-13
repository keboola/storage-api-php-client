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
use Keboola\StorageApi\Tokens;
use Keboola\Test\ClientProvider\ClientProvider;
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

    public function setUp(): void
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
        foreach ($this->tokens->listTokens() as $token) {
            if ($token['isMasterToken']) {
                continue;
            }

            $this->tokens->dropToken($token['id']);
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
            ->setConfiguration(['x' => 'y'])
            ->setName('Main2'));

        $components->addConfiguration((new Configuration())
            ->setComponentId('provisioning')
            ->setConfigurationId('main-1')
            ->setName('Main1'));
    }

    public function testTokenProperties(): void
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

        $this->assertMasterTokenVisibility($currentToken);

        $owner = $currentToken['owner'];
        $this->assertIsInt($owner['dataSizeBytes']);
        $this->assertIsInt($owner['rowsCount']);
        $this->assertIsBool($owner['hasRedshift']);

        $admin = $currentToken['admin'];
        $this->assertIsString($admin['name']);
        $this->assertIsInt($admin['id']);
        $this->assertIsArray($admin['features']);
        $this->assertIsBool($admin['isOrganizationMember']);
        $this->assertEquals('admin', $admin['role']);

        $this->assertArrayHasKey('limits', $owner);
        $this->assertArrayHasKey('metrics', $owner);
        $this->assertArrayHasKey('defaultBackend', $owner);

        $firstLimit = reset($owner['limits']);
        $limitKeys = array_keys($owner['limits']);

        $this->assertArrayHasKey('name', $firstLimit);
        $this->assertArrayHasKey('value', $firstLimit);
        $this->assertIsInt($firstLimit['value']);
        $this->assertEquals($firstLimit['name'], $limitKeys[0]);

        $tokenFound = false;
        foreach ($this->tokens->listTokens() as $token) {
            if ($token['id'] !== $currentToken['id']) {
                continue;
            }

            $this->assertArrayHasKey('admin', $token);

            $admin = $token['admin'];
            $this->assertIsInt($admin['id']);
            $this->assertIsString($admin['name']);
            $this->assertIsString($admin['role']);
            $this->assertEquals('admin', $admin['role']);

            $tokenFound = true;
        }

        $this->assertTrue($tokenFound);

        // check role of guest user
        $guestUserToken = $this->getGuestClient()->verifyToken();

        $this->assertArrayHasKey('admin', $guestUserToken);
        $admin = $guestUserToken['admin'];
        $this->assertEquals('guest', $admin['role']);
    }

    public function testGetToken(): void
    {
        $verifiedToken = $this->_client->verifyToken();
        $currentToken = $this->tokens->getToken($verifiedToken['id']);

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

        $this->assertMasterTokenVisibility($currentToken);

        $owner = $currentToken['owner'];
        $this->assertIsInt($owner['dataSizeBytes']);
        $this->assertIsInt($owner['rowsCount']);
        $this->assertIsBool($owner['hasRedshift']);

        $this->assertArrayHasKey('limits', $owner);
        $this->assertArrayHasKey('metrics', $owner);
        $this->assertArrayHasKey('defaultBackend', $owner);

        $firstLimit = reset($owner['limits']);
        $limitKeys = array_keys($owner['limits']);

        $this->assertArrayHasKey('name', $firstLimit);
        $this->assertArrayHasKey('value', $firstLimit);
        $this->assertIsInt($firstLimit['value']);
        $this->assertEquals($firstLimit['name'], $limitKeys[0]);

        $tokenFound = false;
        foreach ($this->tokens->listTokens() as $token) {
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

    /**
     * @return void
     */
    public function testVerifyTokenForDefaultVsBranch(): void
    {
        // get token for default branch
        $defaultToken = $this->_client->verifyToken();

        // prepare DEV branch client
        $clientProvider = new ClientProvider($this);
        $branchClient = $clientProvider->getDevBranchClient();

        // get token for DEV branch
        $branchToken = $branchClient->verifyToken();

        $this->assertSame($defaultToken, $branchToken);
    }

    /**
     * @return void
     */
    public function testGetTokenForDefaultVsBranch(): void
    {
        $verifiedToken = $this->_client->verifyToken();

        // get token for default branch
        $defaultToken = $this->tokens->getToken($verifiedToken['id']);

        // prepare DEV branch client
        $clientProvider = new ClientProvider($this);
        $branchClient = $clientProvider->getDevBranchClient();
        $branchTokens = new Tokens($branchClient);

        // get token for DEV branch
        $branchToken = $branchTokens->getToken($verifiedToken['id']);

        $this->assertSame($defaultToken, $branchToken);
    }

    public function testPayGoTokenProperties(): void
    {
        $currentToken = $this->_client->verifyToken();

        $this->assertArrayHasKey('owner', $currentToken);

        if (!in_array('pay-as-you-go', $currentToken['owner']['features'])) {
            $this->assertArrayNotHasKey('payAsYouGo', $currentToken['owner']);
            $this->markTestSkipped('Project is not Pay As You Go project');
        } else {
            $this->assertArrayHasKey('payAsYouGo', $currentToken['owner']);

            $payAsYouGo = $currentToken['owner']['payAsYouGo'];
            $this->assertIsFloat($payAsYouGo['purchasedCredits']);
        }
    }

    public function testKeenReadTokensRetrieve(): void
    {
        $this->expectException(\Keboola\StorageApi\ClientException::class);
        $this->expectExceptionMessage('Api endpoint \'storage/tokens/keen\' was removed from KBC');
        $this->_client->getKeenReadCredentials();
    }

    public function testInvalidToken(): void
    {
        $invalidToken = 'thisIsInvalidToken';

        try {
            $client = $this->getClient([
                'token' => $invalidToken,
                'url' => STORAGE_API_URL,
            ]);

            $client->verifyToken();
            $this->fail('Exception should be thrown on invalid token');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertStringNotContainsString($invalidToken, $e->getMessage(), 'Token value should not be returned back');
        }
    }

    public function testInvalidTokenWhenTokenIsFalse(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('resource not found');
        // false is not event sent, because "string" . false = "string"
        /** @phpstan-ignore-next-line */
        $this->tokens->dropToken(false);
    }

    public function testInvalidTokenWhenTokenIsString(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('resource not found');
        /** @phpstan-ignore-next-line */
        $this->tokens->dropToken('foo');
    }

    public function testTokenRefreshWhenTokenIsString(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Argument "id" is expected to be type "int", value "foo" given.');
        /** @phpstan-ignore-next-line */
        $this->tokens->refreshToken('foo');
    }

    public function testTokenGetWhenTokenIsString(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('resource not found');
        /** @phpstan-ignore-next-line */
        $this->tokens->getToken('foo');
    }


    public function testBucketPermissionUpdate(): void
    {
        $options = (new TokenCreateOptions())
            ->setDescription('Some description')
        ;

        $token = $this->tokens->createToken($options);

        $bucketPermissions = $token['bucketPermissions'];
        $this->assertCount(0, $bucketPermissions);

        // read permissions
        $permission = TokenAbstractOptions::BUCKET_PERMISSION_READ;
        $options = (new TokenUpdateOptions($token['id']))
            ->addBucketPermission($this->outBucketId, $permission)
        ;

        $token = $this->tokens->updateToken($options);

        $bucketPermissions = $token['bucketPermissions'];
        $this->assertCount(1, $bucketPermissions);

        $this->assertEquals($this->outBucketId, key($bucketPermissions));
        $this->assertEquals($permission, reset($bucketPermissions));

        // read permissions
        $permission = TokenAbstractOptions::BUCKET_PERMISSION_WRITE;
        $options = (new TokenUpdateOptions($token['id']))
            ->addBucketPermission($this->outBucketId, $permission)
        ;

        $token = $this->tokens->updateToken($options);

        $bucketPermissions = $token['bucketPermissions'];
        $this->assertCount(1, $bucketPermissions);

        $this->assertEquals($this->outBucketId, key($bucketPermissions));
        $this->assertEquals($permission, reset($bucketPermissions));

        // invalid permission
        $options = (new TokenUpdateOptions($token['id']))
            ->addBucketPermission($this->outBucketId, self::BUCKET_PERMISSION_MANAGE)
        ;

        try {
            $this->tokens->updateToken($options);
            $this->fail('Manage permissions should not be allowed to set');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tokens.invalidPermissions', $e->getStringCode());
        }

        $token = $this->tokens->getToken($token['id']);

        $bucketPermissions = $token['bucketPermissions'];
        $this->assertCount(1, $bucketPermissions);

        $this->assertEquals($this->outBucketId, key($bucketPermissions));
        $this->assertEquals($permission, reset($bucketPermissions));
    }

    public function testAssignNonExistingBucketPermissionShouldFail(): void
    {
        $options = (new TokenCreateOptions())
            ->setDescription('Some description')
            ->addBucketPermission($this->outBucketId, self::BUCKET_PERMISSION_MANAGE)
        ;

        try {
            $this->tokens->createToken($options);
            $this->fail('Invalid permissions exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tokens.invalidPermissions', $e->getStringCode());
        }
    }

    public function testTokenDrop(): void
    {
        $initialTokens = $this->tokens->listTokens();

        $token = $this->tokens->createToken(new TokenCreateOptions());

        $tokens = $this->tokens->listTokens();
        $this->assertCount(count($initialTokens) + 1, $tokens);

        $this->tokens->dropToken($token['id']);

        $tokens = $this->tokens->listTokens();
        $this->assertCount(count($initialTokens), $tokens);
    }

    public function testDeleteOwnToken(): void
    {
        $initialTokens = $this->tokens->listTokens();

        $newToken = $this->tokens->createToken(new TokenCreateOptions());

        $tokens = $this->tokens->listTokens();
        $this->assertCount(count($initialTokens) + 1, $tokens);

        $newTokenClient = $this->getClientForToken($newToken['token']);
        $tokens = new Tokens($newTokenClient);
        $tokens->dropToken($newToken['id']);

        $tokens = $this->tokens->listTokens();
        $this->assertCount(count($initialTokens), $tokens);
    }

    public function testTokenDefaultOptions(): void
    {
        $currentToken = $this->_client->verifyToken();

        $token = $this->tokens->createToken(new TokenCreateOptions());

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
        // Default token should return unencrypted token
        $this->assertArrayHasKey('token', $token);

        $this->assertArrayHasKey('creatorToken', $token);

        $creator = $token['creatorToken'];
        $this->assertEquals($currentToken['id'], $creator['id']);
        $this->assertEquals($currentToken['description'], $creator['description']);
    }

    public function testTokenMaximalOptions(): void
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

        $token = $this->tokens->createToken($options);

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

    public function testTokenRefresh(): void
    {
        $options = (new TokenCreateOptions())
            ->setDescription('Out read token')
        ;

        $token = $this->tokens->createToken($options);

        $tokenString = $token['token'];
        $created = new \DateTime($token['created']);

        sleep(1);

        $this->tokens->refreshToken($token['id']);
        $token = $this->tokens->getToken($token['id']);

        $refreshed = new \DateTime($token['refreshed']);

        $this->assertNotEquals($tokenString, $token['token']);
        $this->assertGreaterThan($created->getTimestamp(), $refreshed->getTimestamp());
    }

    public function testTokenWithoutTokensManagePermissionCannotRefreshOtherTokens(): void
    {
        $options = (new TokenCreateOptions())
            ->setDescription('Out read token')
        ;

        $limitedAccessToken = $this->tokens->createToken($options);
        $limitAccessTokenClient = $this->getClient([
            'token' => $limitedAccessToken['token'],
            'url' => STORAGE_API_URL,
        ]);

        $otherToken = $this->tokens->createToken($options);

        $this->expectException(ClientException::class);
        $this->expectExceptionCode(403);

        $limitAccessTokens = new Tokens($limitAccessTokenClient);
        $limitAccessTokens->refreshToken($otherToken['id']);
        $this->assertEquals($limitedAccessToken, $this->tokens->getToken($limitedAccessToken['id']));
    }

    public function testTokenComponentAccess(): void
    {
        $this->clearComponents();
        $this->initTestConfigurations();

        $options = (new TokenCreateOptions())
            ->setDescription('Component Access Test Token')
            ->addComponentAccess('wr-db')
        ;

        $token = $this->tokens->createToken($options);

        $client = $this->getClient([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
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

    public function testTokenComponentAccessError(): void
    {
        $this->clearComponents();
        $this->initTestConfigurations();

        $options = (new TokenCreateOptions())
            ->setDescription('Component Access Test Token')
        ;

        $token = $this->tokens->createToken($options);

        $client = $this->getClient([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ]);

        $components = new Components($client);

        try {
            $components->listComponents();
            $this->fail('This token should not be allowed to access components API');
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
            $this->fail('Token was not granted access to this component, should throw an exception');
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
            $this->fail('Token was not granted access to this component, should throw an exception');
        } catch (ClientException  $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        // grant permission to component
        $options = (new TokenUpdateOptions($token['id']))
            ->addComponentAccess('provisioning')
        ;

        $this->tokens->updateToken($options);

        $componentList = $components->listComponents();
        $this->assertCount(1, $componentList);

        $this->assertEquals('provisioning', $componentList[0]['id']);

        $this->clearComponents();
    }

    public function testBucketReadTokenPermission(): void
    {
        $outTableId = $this->_client->createTableAsync(
            $this->outBucketId,
            'languages',
            new CsvFile(__DIR__ . '/../_data/languages.csv')
        );

        $options = (new TokenCreateOptions())
            ->setDescription('Out read token')
            ->addBucketPermission($this->outBucketId, TokenAbstractOptions::BUCKET_PERMISSION_READ)
        ;

        $token = $this->tokens->createToken($options);

        $client = $this->getClient([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ]);

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
            $client->writeTableAsync($outTableId, new CsvFile(__DIR__ . '/../_data/languages.csv'));
            $this->fail('Table imported with read token');
        } catch (ClientException  $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }
    }

    public function testBucketWriteTokenPermission(): void
    {
        $outTableId = $this->_client->createTableAsync(
            $this->outBucketId,
            'languages',
            new CsvFile(__DIR__ . '/../_data/languages.csv')
        );

        $options = (new TokenCreateOptions())
            ->setDescription('Out write token')
            ->addBucketPermission($this->outBucketId, TokenAbstractOptions::BUCKET_PERMISSION_WRITE)
        ;

        $token = $this->tokens->createToken($options);

        $client = $this->getClient([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ]);

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
        $client->writeTableAsync($outTableId, new CsvFile(__DIR__ . '/../_data/languages.csv'));
    }

    public function testNoBucketTokenPermission(): void
    {
        $outTableId = $this->_client->createTableAsync(
            $this->outBucketId,
            'languages',
            new CsvFile(__DIR__ . '/../_data/languages.csv')
        );

        $options = (new TokenCreateOptions())
            ->setDescription('No bucket permission token')
        ;

        $token = $this->tokens->createToken($options);

        $client = $this->getClient([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ]);

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
            $client->writeTableAsync($outTableId, new CsvFile(__DIR__ . '/../_data/languages.csv'));
            $this->fail('Table imported with no permission token');
        } catch (ClientException  $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }
    }

    public function testAssignNonExistingBucketShouldFail(): void
    {
        $options = (new TokenCreateOptions())
            ->setDescription('Some description')
            ->addBucketPermission('out.non-existing', TokenAbstractOptions::BUCKET_PERMISSION_READ)
        ;

        try {
            $this->tokens->createToken($options);
            $this->fail('Invalid permissions exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tokens.invalidPermissions', $e->getStringCode());
        }
    }

    public function testAllBucketsTokenPermissions(): void
    {
        $bucketsInitialCount = count($this->_client->listBuckets());

        $options = (new TokenCreateOptions())
            ->setDescription('Out buckets manage token')
            ->setCanManageBuckets(true)
        ;

        $token = $this->tokens->createToken($options);

        $client = $this->getClient([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ]);

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

        $token = $this->tokens->getToken($token['id']);
        $this->assertCount($bucketsInitialCount + 1, $token['bucketPermissions']);
        foreach ($token['bucketPermissions'] as $bucketId => $permission) {
            $this->assertEquals(self::BUCKET_PERMISSION_MANAGE, $permission);
        }

        $client->getBucket($newBucketId);
        $client->dropBucket($newBucketId, ['async' => true]);
    }

    public function testTokenWithExpiration(): void
    {
        $options = (new TokenCreateOptions())
            ->setDescription('Out read token with expiration')
            ->setExpiresIn(2 * 60)
        ;

        $token = $this->tokens->createToken($options);

        $client = $this->getClient([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ]);

        $token = $client->verifyToken();

        $this->assertNotEmpty($token['expires']);
        $this->assertFalse($token['isExpired']);
    }

    public function testExpiredToken(): void
    {
        $options = (new TokenCreateOptions())
            ->setDescription('Out read token with expiration')
            ->setExpiresIn(1)
        ;

        $token = $this->tokens->createToken($options);
        $tries = 0;

        $this->expectException(ClientException::class);
        while ($tries < 7) {
            $client = $this->getClient([
                'token' => $token['token'],
                'url' => STORAGE_API_URL,
            ]);
            $client->verifyToken();
            sleep(pow(2, $tries++));
        }

        $this->fail('token should be invalid');
    }

    public function testTokenUpdateKeepsCanManageBucketsFlag(): void
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

        $token = $this->tokens->createToken($options);

        $this->assertTrue($token['canManageBuckets']);

        $this->assertCount(count($bucketIds), $token['bucketPermissions']);
        $this->assertEmpty(array_diff($bucketIds, array_keys($token['bucketPermissions'])));

        foreach ($token['bucketPermissions'] as $bucketPermission) {
            $this->assertSame(self::BUCKET_PERMISSION_MANAGE, $bucketPermission);
        }

        // update token and set buckets permissions
        $options = (new TokenUpdateOptions($token['id']))
            ->setDescription('CanManageBuckets update 1')
            ->addBucketPermission($this->outBucketId, TokenAbstractOptions::BUCKET_PERMISSION_READ)
        ;

        $token = $this->tokens->updateToken($options);

        $this->assertTrue($token['canManageBuckets']);

        $this->assertCount(count($bucketIds), $token['bucketPermissions']);
        $this->assertEmpty(array_diff($bucketIds, array_keys($token['bucketPermissions'])));

        foreach ($token['bucketPermissions'] as $bucketPermission) {
            $this->assertSame(self::BUCKET_PERMISSION_MANAGE, $bucketPermission);
        }

        // update token without setting permissions
        $options = (new TokenUpdateOptions($token['id']))
            ->setDescription('CanManageBuckets update 2')
        ;

        $token = $this->tokens->updateToken($options);

        $this->assertTrue($token['canManageBuckets']);

        $this->assertCount(count($bucketIds), $token['bucketPermissions']);
        $this->assertEmpty(array_diff($bucketIds, array_keys($token['bucketPermissions'])));

        foreach ($token['bucketPermissions'] as $bucketPermission) {
            $this->assertSame(self::BUCKET_PERMISSION_MANAGE, $bucketPermission);
        }
    }

    public function testTokenWithoutTokensManagePermissionCanListAndViewOnlySelf(): void
    {
        $initialTokens = $this->tokens->listTokens();

        $options = (new TokenCreateOptions())
            ->setDescription('Token without canManageTokens permission')
        ;

        $token = $this->tokens->createToken($options);

        $this->assertFalse($token['canManageTokens']);

        $tokensList = $this->tokens->listTokens();
        $this->assertCount(count($initialTokens) + 1, $tokensList);

        $client = $this->getClient([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ]);

        $verifiedToken = $client->verifyToken();

        $tokens = new Tokens($client);
        $tokensList = $tokens->listTokens();
        $this->assertCount(1, $tokensList);

        $token = reset($tokensList);
        $this->assertSame($verifiedToken['id'], $token['id']);

        $token = $tokens->getToken($token['id']);
        $this->assertSame($verifiedToken['id'], $token['id']);

        $options = (new TokenCreateOptions())
            ->setDescription('Token without canManageTokens permission')
        ;

        $token = $this->tokens->createToken($options);

        try {
            $tokens->getToken($token['id']);
            $this->fail('Other token detail with no permissions');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }
    }

    public function testTokenWithoutCanPurgeTrashPermission(): void
    {
        $this->clearComponents();

        // create token without purge trash permission
        $options = (new TokenCreateOptions())
            ->setCanManageBuckets(true)
            ->setDescription('Token without canPurgeTrash permission')
        ;

        $token = $this->tokens->createToken($options);

        $this->assertFalse($token['canPurgeTrash']);

        $client = $this->getClient([
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
        $options = (new TokenUpdateOptions($token['id']))
            ->setCanPurgeTrash(true)
        ;

        $token = $this->tokens->updateToken($options);

        $this->assertTrue($token['canPurgeTrash']);

        $components->deleteConfiguration('provisioning', 'for-delete');

        $configurations = $components->listComponentConfigurations($listDeletedOptions);
        $this->assertCount(0, $configurations);
    }

    public function testTokenEvents(): void
    {
        $options = (new TokenCreateOptions())
            ->setCanManageBuckets(true)
            ->setDescription('Token with canManageBuckets permission')
        ;

        $token = $this->tokens->createToken($options);

        $client = $this->getClient([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ]);

        $event = new Event();
        $event->setComponent('dummy')
            ->setMessage($token['description'] . ' sample event');

        $event = $this->createAndWaitForEvent($event, $client);

        $tokenEvents = $this->_client->listTokenEvents($token['id']);
        $this->assertCount(2, $tokenEvents); // token created + sample event

        $this->assertSame($event, reset($tokenEvents));

        $events = $this->_client->listEvents();
        $this->assertGreaterThan(2, count($events));
    }

    public function testTokenWithoutTokensManagePermissionCanListOnlyOwnTokenEvents(): void
    {
        $options = (new TokenCreateOptions())
            ->setCanManageBuckets(true)
            ->setDescription('Token with canManageBuckets permission')
        ;

        $token = $this->tokens->createToken($options);

        $client = $this->getClient([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ]);

        // check access for own events
        $event = new Event();
        $event->setComponent('dummy')
            ->setMessage($token['description'] . ' sample event');

        $event = $this->createAndWaitForEvent($event, $client);

        $tokenEvents = $client->listTokenEvents($token['id']);
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

    public function testGuestRoleTokenSettings(): void
    {
        $client = $this->getGuestClient();
        $token = $client->verifyToken();

        $this->assertArrayHasKey('admin', $token);
        $this->assertArrayHasKey('role', $token['admin']);

        $this->assertSame('guest', $token['admin']['role']);

        $this->assertArrayHasKey('canManageBuckets', $token);
        $this->assertTrue($token['canManageBuckets']);
        $this->assertArrayHasKey('canManageTokens', $token);
        $this->assertFalse($token['canManageTokens']);
        $this->assertArrayHasKey('canReadAllFileUploads', $token);
        $this->assertTrue($token['canReadAllFileUploads']);
        $this->assertArrayHasKey('canPurgeTrash', $token);
        $this->assertFalse($token['canPurgeTrash']);
    }

    public function testAdminRoleTokenSettings(): void
    {
        $token = $this->_client->verifyToken();

        $this->assertArrayHasKey('admin', $token);
        $this->assertArrayHasKey('role', $token['admin']);

        $this->assertSame('admin', $token['admin']['role']);

        $this->assertArrayHasKey('canManageBuckets', $token);
        $this->assertTrue($token['canManageBuckets']);
        $this->assertArrayHasKey('canManageTokens', $token);
        $this->assertTrue($token['canManageTokens']);
        $this->assertArrayHasKey('canReadAllFileUploads', $token);
        $this->assertTrue($token['canReadAllFileUploads']);
        $this->assertArrayHasKey('canPurgeTrash', $token);
        $this->assertTrue($token['canPurgeTrash']);
    }

    public function testShareRoleTokenSettings(): void
    {
        $client = $this->getClientForToken(STORAGE_API_SHARE_TOKEN);
        $token = $client->verifyToken();

        $this->assertArrayHasKey('admin', $token);
        $this->assertArrayHasKey('role', $token['admin']);

        $this->assertSame('share', $token['admin']['role']);

        $this->assertArrayHasKey('canManageBuckets', $token);
        $this->assertTrue($token['canManageBuckets']);
        $this->assertArrayHasKey('canManageTokens', $token);
        $this->assertTrue($token['canManageTokens']);
        $this->assertArrayHasKey('canReadAllFileUploads', $token);
        $this->assertTrue($token['canReadAllFileUploads']);
        $this->assertArrayHasKey('canPurgeTrash', $token);
        $this->assertTrue($token['canPurgeTrash']);
    }

    public function testReadOnlyRoleTokenSettings(): void
    {
        $client = $this->getClientForToken(STORAGE_API_READ_ONLY_TOKEN);
        $token = $client->verifyToken();

        $this->assertArrayHasKey('admin', $token);
        $this->assertArrayHasKey('role', $token['admin']);

        $this->assertSame('readOnly', $token['admin']['role']);

        $this->assertArrayHasKey('canManageBuckets', $token);
        $this->assertFalse($token['canManageBuckets']);
        $this->assertArrayHasKey('canManageTokens', $token);
        $this->assertFalse($token['canManageTokens']);
        $this->assertArrayHasKey('canReadAllFileUploads', $token);
        $this->assertTrue($token['canReadAllFileUploads']);
        $this->assertArrayHasKey('canPurgeTrash', $token);
        $this->assertFalse($token['canPurgeTrash']);
    }

    public function testReadOnlyRoleBucketsPermissions(): void
    {
        $client = $this->getClientForToken(STORAGE_API_READ_ONLY_TOKEN);
        $bucketsInitialCount = count($this->_client->listBuckets());

        $token = $client->verifyToken();

        $this->assertCount($bucketsInitialCount, $token['bucketPermissions']);
        foreach ($token['bucketPermissions'] as $bucketId => $permission) {
            $this->assertEquals('read', $permission);
        }

        // check assigned buckets
        $buckets = $client->listBuckets();
        $this->assertCount($bucketsInitialCount, $buckets);

        // create new bucket with master token
        $newBucketId = $this->_client->createBucket('test', 'in', 'testing');

        // check if new token has access to token
        $buckets = $client->listBuckets();
        $this->assertCount($bucketsInitialCount + 1, $buckets);

        $token = $client->verifyToken();
        $this->assertCount($bucketsInitialCount + 1, $token['bucketPermissions']);
        foreach ($token['bucketPermissions'] as $bucketId => $permission) {
            $this->assertEquals('read', $permission);
        }

        $client->getBucket($newBucketId);
        $this->_client->dropBucket($newBucketId, ['async' => true]);
    }


    /**
     * @dataProvider limitedTokenOptionsData
     */
    public function testGuestTokenCreateLimitedToken(TokenCreateOptions $options): void
    {
        $client = $this->getGuestClient();

        $creatorToken = $client->verifyToken();

        $this->assertTrue($creatorToken['isMasterToken']);
        $this->assertFalse($creatorToken['canManageTokens']);

        $guestTokens = new Tokens($client);
        $token = $guestTokens->createToken($options);

        $token = $this->tokens->getToken($token['id']);

        if ($options->getDescription()) {
            $this->assertSame($options->getDescription(), $token['description']);
        } else {
            $this->assertSame(sprintf('Created by %s', $creatorToken['description']), $token['description']);
        }

        if ($options->getExpiresIn()) {
            $this->assertNotEmpty($token['expires']);
        } else {
            $this->assertNull($token['expires']);
        }

        $this->assertFalse($token['isMasterToken']);
        $this->assertFalse($token['canManageBuckets']);
        $this->assertFalse($token['canManageTokens']);
        $this->assertFalse($token['canReadAllFileUploads']);
        $this->assertFalse($token['canPurgeTrash']);
        $this->assertArrayNotHasKey('componentAccess', $token);
        $this->assertSame([], $token['bucketPermissions']);
    }

    /**
     * @dataProvider limitedTokenOptionsData
     */
    public function testReadOnlyUserCannotCreateLimitedToken(TokenCreateOptions $options): void
    {
        $client = $this->getClientForToken(STORAGE_API_READ_ONLY_TOKEN);

        $this->expectExceptionCode(403);
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('You don\'t have access to the resource.');

        $tokens = new Tokens($client);
        $tokens->createToken($options);
    }

    public function limitedTokenOptionsData()
    {
        return [
            'minimal configuration' => [
                (new TokenCreateOptions()),
            ],
            'all applicable params' => [
                (new TokenCreateOptions())
                    ->setDescription('Autosave test')
                    ->setExpiresIn(60 * 5),
            ],
            'full configuration' => [
                (new TokenCreateOptions())
                    ->setDescription('Autosave test')
                    ->setExpiresIn(60 * 5)
                    ->setCanReadAllFileUploads(true)
                    ->setCanPurgeTrash(true)
                    ->addBucketPermission('in.c-test', TokenAbstractOptions::BUCKET_PERMISSION_READ)
                    ->addComponentAccess('wr-db'),
            ],
        ];
    }



    /**
     * @dataProvider provideInvalidOptionsForGuestUser
     * @param class-string<\Throwable> $expectedExceptionClass
     * @param string $expectedExceptionMessage
     */
    public function testGuestUserSuppliesInvalidOptions(
        TokenCreateOptions $invalidOptions,
        $expectedExceptionClass,
        $expectedExceptionMessage
    ): void {
        $client = $this->getGuestClient();

        $token = $client->verifyToken();

        $this->assertTrue($token['isMasterToken']);
        $this->assertFalse($token['canManageTokens']);

        $this->expectException($expectedExceptionClass);
        $this->expectExceptionMessage($expectedExceptionMessage);

        $guestTokens = new Tokens($client);
        $guestTokens->createToken($invalidOptions);
    }

    public function testTokenRefreshesSelf(): void
    {
        $options = (new TokenCreateOptions())
            ->setDescription(__METHOD__)
            ->setExpiresIn(60 * 5)
        ;

        $token = $this->tokens->createToken($options);

        $client = $this->getClientForToken($token['token']);

        $oldTokenData = $client->verifyToken();
        $this->assertTrue($token['token'] === $oldTokenData['token']);
        $this->assertSame($token['id'], $oldTokenData['id']);

        sleep(2);

        $client->refreshToken();

        $newTokenData = $client->verifyToken();
        $this->assertTrue($token['token'] !== $newTokenData['token']);
        $this->assertNotSame($oldTokenData['refreshed'], $newTokenData['refreshed']);
        $this->assertSame($token['id'], $newTokenData['id']);

        // Token refresh via Tokens class does not affects current token in Client
        $tokens = new Tokens($client);
        $tokens->refreshToken($token['id']);

        $this->expectException(ClientException::class);
        $this->expectExceptionCode(401);
        $client->verifyToken();
    }

    public function testDeprecatedMethods(): void
    {
        $initialTokens = $this->_client->listTokens();

        // token create
        $options = (new TokenCreateOptions())
            ->setDescription(__METHOD__)
            ->setExpiresIn(60 * 5)
        ;

        $tokenId = $this->_client->createToken($options);

        // tokens list
        $tokens = $this->_client->listTokens();
        $this->assertCount(count($initialTokens) + 1, $tokens);

        // token detail
        $oldTokenData = $this->_client->getToken($tokenId);
        $this->assertSame($tokenId, $oldTokenData['id']);

        // token update
        $options = (new TokenUpdateOptions($tokenId))
            ->setDescription(__METHOD__ . ' updated');

        $tokenId = (string) $this->_client->updateToken($options);
        $this->assertSame($oldTokenData['id'], $tokenId);

        $newTokenData = $this->_client->getToken($tokenId);
        $this->assertNotSame($oldTokenData['description'], $newTokenData['description']);

        // token share
        $this->_client->shareToken($tokenId, 'test@devel.keboola.com', 'Hi');

        // token refresh
        $newToken = $this->_client->refreshToken($tokenId);

        $client = $this->getClientForToken($newToken);
        $client->verifyToken();

        // token drop
        $result = $this->_client->dropToken($tokenId);
        $this->assertSame('', $result);

        $tokens = $this->_client->listTokens();
        $this->assertCount(count($initialTokens), $tokens);
    }

    public function testCreateTokenWithCanCreateJobsFlagOnNonSOXProjects(): void
    {
        // flag has no use in non-sox projects, but token can be created
        $tokenWithCreateJobsFlag = (new Tokens($this->_client))
            ->createToken(
                (new TokenCreateOptions())->setCanCreateJobs(true)
            );
        $this->assertTrue($tokenWithCreateJobsFlag['canCreateJobs']);
    }

    public function provideInvalidOptionsForGuestUser()
    {
        yield 'invalid expiration' => [
            (new TokenCreateOptions())
                ->setExpiresIn(0)
                ->setDescription('Whatever'),
            ClientException::class,
            'Minimal expiration must be greater or equal to 1 second(s)',
        ];
    }

    public function getGuestClient()
    {
        $client = $this->getClient([
            'token' => STORAGE_API_GUEST_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ]);
        return $client;
    }

    private function assertMasterTokenVisibility(array $currentToken)
    {
        $owner = $currentToken['owner'];
        $this->assertArrayHasKey('features', $owner);
        $features = $owner['features'];

        if (in_array('force-decrypted-token', $features)) {
            // when force is active must show token in any case
            $this->assertArrayHasKey('token', $currentToken);
        } else {
            if (in_array('hide-decrypted-token', $features)) {
                // when has hide feature token is hidden
                $this->assertArrayNotHasKey('token', $currentToken);
            } else {
                // when does not have hide feature token is shown
                $this->assertArrayHasKey('token', $currentToken);
            }
        }
    }
}
