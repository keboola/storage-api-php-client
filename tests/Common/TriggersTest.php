<?php



namespace Keboola\Test\Common;

use Exception;
use Generator;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\TokenAbstractOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\Test\StorageApiTestCase;

class TriggersTest extends StorageApiTestCase
{
    /**
     * @return void
     */
    public function setUp(): void
    {
        parent::setUp();

        $this->_initEmptyTestBuckets();

        $triggers = $this->_client->listTriggers();
        foreach ($triggers as $trigger) {
            $this->_client->deleteTrigger((int) $trigger['id']);
        }
    }

    public function testCannotCreateTriggerWithZeroCooldown(): void
    {
        $table1 = $this->createTableWithRandomData('watched-1');
        $options = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ);
        $newToken = $this->tokens->createToken($options);
        $this->expectExceptionMessage('Minimal cool down period is 1 minute');
        $this->expectException(ClientException::class);
        $this->_client->createTrigger([
            'component' => 'orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 0,
            'runWithTokenId' => $newToken['id'],
            'tableIds' => [
                $table1,
            ],
        ]);
    }

    /**
     * @return void
     */
    public function testCreateTrigger(): void
    {
        $table1 = $this->createTableWithRandomData('watched-1');
        $options = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ);
        $newToken = $this->tokens->createToken($options);
        $trigger = $this->_client->createTrigger([
            'component' => 'orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 1,
            'runWithTokenId' => $newToken['id'],
            'tableIds' => [
                $table1,
            ],
        ]);

        $this->assertEquals('orchestrator', $trigger['component']);
        $this->assertEquals(123, $trigger['configurationId']);
        $this->assertEquals(1, $trigger['coolDownPeriodMinutes']);
        $this->assertEquals($newToken['id'], $trigger['runWithTokenId']);
        $this->assertNotNull($trigger['lastRun']);
        $this->assertLessThan((new \DateTime()), (new \DateTime($trigger['lastRun'])));
        $this->assertEquals(
            [
                ['tableId' => 'in.c-API-tests.watched-1'],
            ],
            $trigger['tables'],
        );
        $token = $this->_client->verifyToken();
        $this->assertEquals(
            [
                'id' => $token['id'],
                'description' => $token['description'],
            ],
            $trigger['creatorToken'],
        );
    }


    /**
     * @return void
     */
    public function testCreateTriggerStringConfiguration(): void
    {
        $table1 = $this->createTableWithRandomData('watched-1');
        $options = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ);
        $newToken = $this->tokens->createToken($options);
        $trigger = $this->_client->createTrigger([
            'component' => 'orchestrator',
            'configurationId' => 'my-configuration',
            'coolDownPeriodMinutes' => 1,
            'runWithTokenId' => $newToken['id'],
            'tableIds' => [
                $table1,
            ],
        ]);

        $this->assertEquals('orchestrator', $trigger['component']);
        $this->assertEquals('my-configuration', $trigger['configurationId']);
        $this->assertEquals(1, $trigger['coolDownPeriodMinutes']);
        $this->assertEquals($newToken['id'], $trigger['runWithTokenId']);
        $this->assertNotNull($trigger['lastRun']);
        $this->assertLessThan((new \DateTime()), (new \DateTime($trigger['lastRun'])));
        $this->assertEquals(
            [
                ['tableId' => 'in.c-API-tests.watched-1'],
            ],
            $trigger['tables'],
        );
        $token = $this->_client->verifyToken();
        $this->assertEquals(
            [
                'id' => $token['id'],
                'description' => $token['description'],
            ],
            $trigger['creatorToken'],
        );
    }

    /**
     * @return void
     */
    public function testCreateTriggerAsNonAdminButWithMasterTokenAsTokenRunWith(): void
    {
        $table1 = $this->createTableWithRandomData('watched-1');
        $options = (new TokenCreateOptions())
            ->setCanManageBuckets(true);
        $newToken = $this->tokens->createToken($options);
        $clientWithoutAdminToken = $this->getClient(['url' => STORAGE_API_URL, 'token' => $newToken['token']]);
        try {
            $clientWithoutAdminToken->createTrigger([
                'component' => 'orchestrator',
                'configurationId' => 123,
                'coolDownPeriodMinutes' => 10,
                // using master token as runWithTokenId but client's token isn't master
                'runWithTokenId' => $this->_client->verifyToken()['id'],
                'tableIds' => [
                    $table1,
                ],
            ]);
            self::fail('should fail');
        } catch (ClientException $e) {
            $this->assertEquals(
                "The 'runByToken' cannot be admin's token when your main token is not admin's",
                $e->getMessage(),
            );
        }
    }

    /**
     * @dataProvider tokenCreateOptionsProvider
     * @return void
     */
    public function testCreateTriggerWithExtraPermissions(TokenCreateOptions $optionsForMainToken): void
    {
        $table1 = $this->createTableWithRandomData('watched-1');

        $optionsForTokenRunWith = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ);

        $tokenRunWith = $this->tokens->createToken($optionsForTokenRunWith);
        $newNonAdminToken = $this->tokens->createToken($optionsForMainToken);

        $clientWithoutAdminToken = $this->getClient(['url' => STORAGE_API_URL, 'token' => $newNonAdminToken['token']]);
        $trigger = $clientWithoutAdminToken->createTrigger([
            'component' => 'keboola.orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $tokenRunWith['id'],
            'tableIds' => [
                $table1,
            ],
        ]);

        $this->assertEquals('keboola.orchestrator', $trigger['component']);
        $this->assertEquals(123, $trigger['configurationId']);
        $this->assertEquals(10, $trigger['coolDownPeriodMinutes']);
        $this->assertEquals($tokenRunWith['id'], $trigger['runWithTokenId']);
        $this->assertNotNull($trigger['lastRun']);
        $this->assertLessThan((new \DateTime()), (new \DateTime($trigger['lastRun'])));
        $this->assertEquals(
            [
                ['tableId' => 'in.c-API-tests.watched-1'],
            ],
            $trigger['tables'],
        );
        $token = $clientWithoutAdminToken->verifyToken();
        $this->assertEquals(
            [
                'id' => $token['id'],
                'description' => $token['description'],
            ],
            $trigger['creatorToken'],
        );
    }

    /**
     * @dataProvider tokenOptionsProviderInvalid
     * @param TokenCreateOptions $optionsForMainToken
     * @param string $expectedException
     * @return void
     */
    public function testCreateTriggerWithWrongPermissions(TokenCreateOptions $optionsForMainToken, $expectedException): void
    {
        $table1 = $this->createTableWithRandomData('watched-1');

        $optionsForTokenRunWith = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ);

        try {
            $tokenRunWith = $this->tokens->createToken($optionsForTokenRunWith);
            $newNonAdminToken = $this->tokens->createToken($optionsForMainToken);

            $clientWithoutAdminToken = $this->getClient([
                'url' => STORAGE_API_URL,
                'token' => $newNonAdminToken['token'],
            ]);
            $clientWithoutAdminToken->createTrigger([
                'component' => 'keboola.orchestrator',
                'configurationId' => 123,
                'coolDownPeriodMinutes' => 10,
                'runWithTokenId' => $tokenRunWith['id'],
                'tableIds' => [
                    $table1,
                ],
            ]);
            self::fail('should fail before');
        } catch (\Exception $e) {
            self::assertEquals($expectedException, $e->getMessage());
        }
    }

    /**
     * @return void
     */
    public function testCreateTriggerWithMasterTokensEveryWhere(): void
    {
        $table1 = $this->createTableWithRandomData('watched-1');
        $myTokenId = $this->_client->verifyToken()['id'];
        $trigger = $this->_client->createTrigger([
            'component' => 'orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $myTokenId,
            'tableIds' => [
                $table1,
            ],
        ]);

        $this->assertEquals('orchestrator', $trigger['component']);
        $this->assertEquals(123, $trigger['configurationId']);
        $this->assertEquals(10, $trigger['coolDownPeriodMinutes']);
        $this->assertEquals($myTokenId, $trigger['runWithTokenId']);
        $this->assertEquals([['tableId' => 'in.c-API-tests.watched-1']], $trigger['tables']);

        $updateData = [
            'component' => 'keboola.ex-1',
            'configurationId' => 111,
            'coolDownPeriodMinutes' => 20,
            'runWithTokenId' => $myTokenId,
            'tableIds' => [$table1],
        ];
        $updatedTrigger = $this->_client->updateTrigger((int) $trigger['id'], $updateData);
        $this->assertEquals('keboola.ex-1', $updatedTrigger['component']);
        $this->assertEquals(111, $updatedTrigger['configurationId']);
        $this->assertEquals(20, $updatedTrigger['coolDownPeriodMinutes']);
        $this->assertEquals($myTokenId, $updatedTrigger['runWithTokenId']);
        $this->assertEquals([['tableId' => 'in.c-API-tests.watched-1']], $updatedTrigger['tables']);
    }

    /**
     * @dataProvider tokenCreateOptionsProvider
     * @return void
     */
    public function testUpdateTriggerCreatedByMasterToken(TokenCreateOptions $optionForToken): void
    {
        $table1 = $this->createTableWithRandomData('watched-1');
        $table2 = $this->createTableWithRandomData('watched-2');

        $options = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ);

        $newToken = $this->tokens->createToken($options);

        $trigger = $this->_client->createTrigger([
            'component' => 'keboola.orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $newToken['id'],
            'tableIds' => [
                $table1,
                $table2,
            ],
        ]);

        $options = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ);

        $brandNewToken = $this->tokens->createToken($options);

        $updateData = [
            'component' => 'keboola.ex-1',
            'configurationId' => 111,
            'coolDownPeriodMinutes' => 20,
            'runWithTokenId' => $brandNewToken['id'],
            'tableIds' => [$table1],
        ];

        $newNonAdminTokenWithoutPermissions = $this->tokens->createToken((new TokenCreateOptions()));
        $clientWithoutAdminTokenWithoutPermissions = $this->getClient(['url' => STORAGE_API_URL, 'token' => $newNonAdminTokenWithoutPermissions['token']]);

        // try to update the trigger with non-master token without permissions
        try {
            $clientWithoutAdminTokenWithoutPermissions->updateTrigger((int) $trigger['id'], $updateData);
            self::fail('should fail');
        } catch (Exception $e) {
            self::assertEquals('Your token does not have sufficient privilege. Token is neither admin token nor does it have canManageBucket permission nor does it have access to component(s) "keboola.ex-1, keboola.orchestrator".', $e->getMessage());
        }

        $newNonAdminToken = $this->tokens->createToken($optionForToken);
        $clientWithoutAdminToken = $this->getClient(['url' => STORAGE_API_URL, 'token' => $newNonAdminToken['token']]);

        // update the trigger with non-master token
        $updateTrigger = $clientWithoutAdminToken->updateTrigger((int) $trigger['id'], $updateData);

        $this->assertEquals('keboola.ex-1', $updateTrigger['component']);
        $this->assertEquals(111, $updateTrigger['configurationId']);
        $this->assertEquals(20, $updateTrigger['coolDownPeriodMinutes']);
        $this->assertEquals($brandNewToken['id'], $updateTrigger['runWithTokenId']);
        $this->assertEquals([['tableId' => 'in.c-API-tests.watched-1']], $updateTrigger['tables']);

        $updateData = [
            'component' => 'keboola.ex-2',
            'configurationId' => 543,
            'coolDownPeriodMinutes' => 15,
            'runWithTokenId' => $brandNewToken['id'],
            'tableIds' => [$table2],
        ];

        // update the trigger with admin token
        $updateTrigger = $this->_client->updateTrigger((int) $trigger['id'], $updateData);

        $this->assertEquals('keboola.ex-2', $updateTrigger['component']);
        $this->assertEquals(543, $updateTrigger['configurationId']);
        $this->assertEquals(15, $updateTrigger['coolDownPeriodMinutes']);
        $this->assertEquals($brandNewToken['id'], $updateTrigger['runWithTokenId']);
        $this->assertEquals([['tableId' => 'in.c-API-tests.watched-2']], $updateTrigger['tables']);
    }

    public function testUpdateTriggerParameterCreatedByAdminToken(): void
    {
        $table1 = $this->createTableWithRandomData('watched-1');

        $options = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ);
        $tokenRunWith = $this->tokens->createToken($options);

        $trigger = $this->_client->createTrigger([
            'component' => 'keboola.ex-1',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $tokenRunWith['id'],
            'tableIds' => [
                $table1,
            ],
        ]);

        $updateData = [
            'component' => 'keboola.ex-1',
            'configurationId' => 543,
            'coolDownPeriodMinutes' => 15,
            'runWithTokenId' => $tokenRunWith['id'],
            'tableIds' => [$table1],
        ];

        $options = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ);
        $newNonAdminTokenWithoutComponentAccess = $this->tokens->createToken(($options));
        $clientWithoutAdminTokenWithoutComponentAccess = $this->getClient(['url' => STORAGE_API_URL, 'token' => $newNonAdminTokenWithoutComponentAccess['token']]);

        try {
            $clientWithoutAdminTokenWithoutComponentAccess->updateTrigger((int) $trigger['id'], $updateData);
            self::fail('should fail before');
        } catch (\Exception $e) {
            self::assertEquals('Your token does not have sufficient privilege. Token is neither admin token nor does it have canManageBucket permission nor does it have access to component(s) "keboola.ex-1".', $e->getMessage());
        }

        $options = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ)
            ->addComponentAccess('keboola.ex-1');
        $newNonAdminTokenWithPermissions = $this->tokens->createToken(($options));
        $clientWithoutAdminTokenWithPermissions = $this->getClient(['url' => STORAGE_API_URL, 'token' => $newNonAdminTokenWithPermissions['token']]);

        $updatedTrigger = $clientWithoutAdminTokenWithPermissions->updateTrigger((int) $trigger['id'], $updateData);

        $this->assertEquals('keboola.ex-1', $updatedTrigger['component']);
        $this->assertEquals(543, $updatedTrigger['configurationId']);
        $this->assertEquals(15, $updatedTrigger['coolDownPeriodMinutes']);
        $this->assertEquals($tokenRunWith['id'], $updatedTrigger['runWithTokenId']);
        $this->assertEquals([['tableId' => 'in.c-API-tests.watched-1']], $updatedTrigger['tables']);
    }

    /**
     * @dataProvider tokenUpdateOptionsProviderInvalid
     * @param TokenCreateOptions $optionsForMainToken
     * @param string $expectedException
     * @return void
     */
    public function testUpdateTriggerComponentWithWrongPermissions(TokenCreateOptions $optionsForMainToken, $expectedException): void
    {
        $table1 = $this->createTableWithRandomData('watched-1');
        $optionsForTokenRunWith = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ);

        $tokenRunWith = $this->tokens->createToken($optionsForTokenRunWith);

        $trigger = $this->_client->createTrigger([
            'component' => 'keboola.orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $tokenRunWith['id'],
            'tableIds' => [
                $table1,
            ],
        ]);

        $updateData = [
            'component' => 'keboola.ex-1',
            'configurationId' => 111,
            'coolDownPeriodMinutes' => 20,
            'runWithTokenId' => $tokenRunWith['id'],
            'tableIds' => [$table1],
        ];

        try {
            $newNonAdminToken = $this->tokens->createToken($optionsForMainToken);
            $clientWithoutAdminToken = $this->getClient([
                'url' => STORAGE_API_URL,
                'token' => $newNonAdminToken['token'],
            ]);

            $clientWithoutAdminToken->updateTrigger((int) $trigger['id'], $updateData);
            self::fail('should fail before');
        } catch (\Exception $e) {
            self::assertEquals($expectedException, $e->getMessage());
        }
    }

    public function tokenUpdateOptionsProviderInvalid(): Generator
    {
        $this->setUp();
        yield 'Token has only access on current set component' => [
            (new TokenCreateOptions())
                ->addComponentAccess('keboola.orchestrator'),
            'Your token does not have sufficient privilege. Token is neither admin token nor does it have canManageBucket permission nor does it have access to component(s) "keboola.ex-1".',
        ];

        yield 'Token has only access on to update set component' => [
            (new TokenCreateOptions())
                ->addComponentAccess('keboola.ex-1'),
            'Your token does not have sufficient privilege. Token is neither admin token nor does it have canManageBucket permission nor does it have access to component(s) "keboola.orchestrator".',
        ];
    }

    /**
     * @dataProvider tokenCreateOptionsProvider
     * @return void
     */
    public function testUpdateTriggerCreatedByNonMasterToken(TokenCreateOptions $options): void
    {
        $table1 = $this->createTableWithRandomData('watched-1');
        $table2 = $this->createTableWithRandomData('watched-2');

        $optionsForTokenRunWith = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ);

        $tokenRunWith = $this->tokens->createToken($optionsForTokenRunWith);
        $newNonAdminToken = $this->tokens->createToken($options);
        $clientWithoutAdminToken = $this->getClient(['url' => STORAGE_API_URL, 'token' => $newNonAdminToken['token']]);

        $trigger = $clientWithoutAdminToken->createTrigger([
            'component' => 'keboola.orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $tokenRunWith['id'],
            'tableIds' => [
                $table1,
                $table2,
            ],
        ]);

        $options = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ);

        $brandNewToken = $this->tokens->createToken($options);

        // check update trigger as non-master but my runWithToken is master
        try {
            $updateData = [
                'component' => 'keboola.ex-1',
                'configurationId' => 111,
                'coolDownPeriodMinutes' => 20,
                'runWithTokenId' => $this->_client->verifyToken()['id'],
                'tableIds' => [$table1],
            ];

            $clientWithoutAdminToken->updateTrigger((int) $trigger['id'], $updateData);
            self::fail('should fail');
        } catch (ClientException $e) {
            $this->assertEquals(
                "The 'runByToken' cannot be admin's token when your main token is not admin's",
                $e->getMessage(),
            );
        }

        $updateData = [
            'component' => 'keboola.ex-1',
            'configurationId' => 111,
            'coolDownPeriodMinutes' => 20,
            'runWithTokenId' => $brandNewToken['id'],
            'tableIds' => [$table1],
        ];

        $updatedTrigger = $clientWithoutAdminToken->updateTrigger((int) $trigger['id'], $updateData);

        $this->assertEquals('keboola.ex-1', $updatedTrigger['component']);
        $this->assertEquals(111, $updatedTrigger['configurationId']);
        $this->assertEquals(20, $updatedTrigger['coolDownPeriodMinutes']);
        $this->assertEquals($brandNewToken['id'], $updatedTrigger['runWithTokenId']);
        $this->assertEquals([['tableId' => 'in.c-API-tests.watched-1']], $updatedTrigger['tables']);

        // try to update trigger with non-master without permissions
        $anotherToken = $this->tokens->createToken((new TokenCreateOptions()));
        $anotherClientWithAnotherTokenWithoutPermission = $this->getClient([
            'url' => STORAGE_API_URL,
            'token' => $anotherToken['token'],
        ]);

        try {
            $anotherClientWithAnotherTokenWithoutPermission->updateTrigger((int) $trigger['id'], $updateData);
            self::fail('should fail');
        } catch (Exception $e) {
            self::assertEquals('Your token does not have sufficient privilege. Token is neither admin token nor does it have canManageBucket permission nor does it have access to component(s) "keboola.ex-1".', $e->getMessage());
        }

        $updateData = [
            'component' => 'keboola.ex-2',
            'configurationId' => 321,
            'coolDownPeriodMinutes' => 12,
            'runWithTokenId' => $brandNewToken['id'],
            'tableIds' => [$table2],
        ];

        // update trigger with master token
        $updatedTrigger = $this->_client->updateTrigger((int) $trigger['id'], $updateData);
        self::assertEquals('keboola.ex-2', $updatedTrigger['component']);
        self::assertEquals(321, $updatedTrigger['configurationId']);
        self::assertEquals(12, $updatedTrigger['coolDownPeriodMinutes']);
        self::assertEquals($brandNewToken['id'], $updatedTrigger['runWithTokenId']);
        self::assertEquals([['tableId' => 'in.c-API-tests.watched-2']], $updatedTrigger['tables']);
    }

    /**
     * @dataProvider tokenCreateOptionsProvider
     * @return void
     */
    public function testUpdateTriggerWithDifferentNonMasterToken(TokenCreateOptions $tokenCreateOptions): void
    {
        $table1 = $this->createTableWithRandomData('watched-1');

        $optionsForTokenRunWith = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ);

        $tokenRunWith = $this->tokens->createToken($optionsForTokenRunWith);
        $newNonAdminToken = $this->tokens->createToken((new TokenCreateOptions())->setCanManageBuckets(true));
        $clientWithoutAdminToken = $this->getClient(['url' => STORAGE_API_URL, 'token' => $newNonAdminToken['token']]);

        $trigger = $clientWithoutAdminToken->createTrigger([
            'component' => 'keboola.orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $tokenRunWith['id'],
            'tableIds' => [$table1],
        ]);

        $options = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ);

        $brandNewToken = $this->tokens->createToken($options);

        $updateData = [
            'component' => 'keboola.ex-1',
            'configurationId' => 111,
            'coolDownPeriodMinutes' => 20,
            'runWithTokenId' => $brandNewToken['id'],
            'tableIds' => [$table1],
        ];

        $newNonAdminTokenDifferent = $this->tokens->createToken($tokenCreateOptions);
        $clientWithDifferentNonAdminToken = $this->getClient(['url' => STORAGE_API_URL, 'token' => $newNonAdminTokenDifferent['token']]);

        $updatedTrigger = $clientWithDifferentNonAdminToken->updateTrigger((int) $trigger['id'], $updateData);
        self::assertEquals('keboola.ex-1', $updatedTrigger['component']);
        self::assertEquals(111, $updatedTrigger['configurationId']);
        self::assertEquals(20, $updatedTrigger['coolDownPeriodMinutes']);
        self::assertEquals($brandNewToken['id'], $updatedTrigger['runWithTokenId']);
        self::assertEquals([['tableId' => 'in.c-API-tests.watched-1']], $updatedTrigger['tables']);
    }

    /**
     * @return void
     */
    public function testUpdateTwoTables(): void
    {
        $table1 = $this->createTableWithRandomData('watched-1');
        $table2 = $this->createTableWithRandomData('watched-2');

        $options = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ);
        $newToken = $this->tokens->createToken($options);

        $trigger1ConfigurationId = time();
        $componentName = uniqid('test', true);
        $trigger1Config = [
            'component' => $componentName,
            'configurationId' => $trigger1ConfigurationId,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $newToken['id'],
            'tableIds' => [
                $table1,
            ],
        ];
        $trigger1 = $this->_client->createTrigger($trigger1Config);
        $trigger2Config = [
            'component' => 'keboola.ex-manzelka',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $newToken['id'],
            'tableIds' => [
                $table2,
            ],
        ];
        $trigger2 = $this->_client->createTrigger($trigger2Config);

        $trigger1 = $this->_client->updateTrigger($trigger1['id'], $trigger1Config);
        $trigger2 = $this->_client->updateTrigger($trigger2['id'], $trigger2Config);

        $triggers = $this->_client->listTriggers();
        $trigger1Found = $trigger2Found = false;
        foreach ($triggers as $trigger) {
            if ($trigger1['id'] === $trigger['id']) {
                $trigger1Found = true;
                $this->assertSame($trigger1Config['tableIds'][0], $trigger['tables'][0]['tableId']);
            }
            if ($trigger2['id'] === $trigger['id']) {
                $trigger2Found = true;
                $this->assertSame($trigger2Config['tableIds'][0], $trigger['tables'][0]['tableId']);
            }
        }

        $this->assertTrue($trigger1Found);
        $this->assertTrue($trigger2Found);
    }

    /**
     * @dataProvider tokenCreateOptionsProvider
     * @return void
     */
    public function testDeleteTriggerCreatedByMasterToken(TokenCreateOptions $optionForToken): void
    {
        $table = $this->createTableWithRandomData('watched-2');

        $options = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ);

        $newToken = $this->tokens->createToken($options);

        $trigger = $this->_client->createTrigger([
            'component' => 'keboola.orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $newToken['id'],
            'tableIds' => [
                $table,
            ],
        ]);

        $trigger2 = $this->_client->createTrigger([
            'component' => 'keboola.orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $newToken['id'],
            'tableIds' => [
                $table,
            ],
        ]);

        $newNonAdminTokenWithoutPermission = $this->tokens->createToken((new TokenCreateOptions()));
        $clientWithoutAdminTokenWithoutPermission = $this->getClient(['url' => STORAGE_API_URL, 'token' => $newNonAdminTokenWithoutPermission['token']]);

        // try to delete the trigger with non-master token without required permission
        try {
            $clientWithoutAdminTokenWithoutPermission->deleteTrigger((int) $trigger['id']);
            self::fail('should fail');
        } catch (Exception $e) {
            self::assertEquals('Your token does not have sufficient privilege. Token is neither admin token nor does it have canManageBucket permission nor does it have access to component(s) "keboola.orchestrator".', $e->getMessage());
        }

        $anotherToken = $this->tokens->createToken($optionForToken);
        $anotherClientWithAnotherToken = $this->getClient([
            'url' => STORAGE_API_URL,
            'token' => $anotherToken['token'],
        ]);

        // trigger can be deleted by non-master token with right privilege
        $anotherClientWithAnotherToken->deleteTrigger((int) $trigger['id']);
        $triggerList = $this->_client->listTriggers();
        $this->assertNotContainsTriggerId($trigger['id'], $triggerList);
        $this->assertCount(1, $triggerList);

        $this->_client->deleteTrigger((int) $trigger2['id']);
        $triggerList = $this->_client->listTriggers();
        $this->assertNotContainsTriggerId($trigger2['id'], $triggerList);

        $this->assertCount(0, $triggerList);
    }

    /**
     * @dataProvider tokenCreateOptionsProvider
     * @return void
     */
    public function testDeleteTriggerCreatedByNonMasterTokenUsingDifferentToken(TokenCreateOptions $optionForToken): void
    {
        $table1 = $this->createTableWithRandomData('watched-1');

        $optionsForTokenRunWith = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ);
        $optionsForMainToken = (new TokenCreateOptions())->setCanManageBuckets(true);

        $tokenRunWith = $this->tokens->createToken($optionsForTokenRunWith);
        $newNonAdminToken = $this->tokens->createToken($optionsForMainToken);
        $clientWithoutAdminToken = $this->getClient(['url' => STORAGE_API_URL, 'token' => $newNonAdminToken['token']]);

        $trigger = $clientWithoutAdminToken->createTrigger([
            'component' => 'keboola.orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $tokenRunWith['id'],
            'tableIds' => [$table1],
        ]);
        $trigger2 = $clientWithoutAdminToken->createTrigger([
            'component' => 'keboola.orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $tokenRunWith['id'],
            'tableIds' => [$table1],
        ]);

        $newNonAdminTokenWithoutPermission = $this->tokens->createToken((new TokenCreateOptions()));
        $clientWithoutAdminTokenWithoutPermission = $this->getClient(['url' => STORAGE_API_URL, 'token' => $newNonAdminTokenWithoutPermission['token']]);

        // try to delete the trigger with non-master token without required permission
        try {
            $clientWithoutAdminTokenWithoutPermission->deleteTrigger((int) $trigger['id']);
            self::fail('should fail');
        } catch (Exception $e) {
            self::assertEquals('Your token does not have sufficient privilege. Token is neither admin token nor does it have canManageBucket permission nor does it have access to component(s) "keboola.orchestrator".', $e->getMessage());
        }

        $anotherToken = $this->tokens->createToken($optionForToken);
        $anotherClientWithAnotherToken = $this->getClient([
            'url' => STORAGE_API_URL,
            'token' => $anotherToken['token'],
        ]);

        // trigger can be deleted by non-master token with right privilege
        $anotherClientWithAnotherToken->deleteTrigger((int) $trigger['id']);
        $triggerList = $clientWithoutAdminToken->listTriggers();
        $this->assertNotContainsTriggerId($trigger['id'], $triggerList);
        $this->assertCount(1, $triggerList);

        // master token can delete it anyway
        $this->_client->deleteTrigger((int) $trigger2['id']);
        $triggerList = $clientWithoutAdminToken->listTriggers();
        $this->assertNotContainsTriggerId($trigger2['id'], $triggerList);

        $this->assertCount(0, $triggerList);
    }

    /**
     * @dataProvider tokenCreateOptionsProvider
     * @return void
     */
    public function testNonMasterTokenCanDeleteTriggerItCreated(TokenCreateOptions $optionForToken): void
    {
        $table1 = $this->createTableWithRandomData('watched-1');

        $optionsForTokenRunWith = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ);

        $tokenRunWith = $this->tokens->createToken($optionsForTokenRunWith);
        $newNonAdminToken = $this->tokens->createToken($optionForToken);
        $clientWithoutAdminToken = $this->getClient(['url' => STORAGE_API_URL, 'token' => $newNonAdminToken['token']]);

        $trigger = $clientWithoutAdminToken->createTrigger([
            'component' => 'keboola.orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $tokenRunWith['id'],
            'tableIds' => [$table1],
        ]);

        // trigger can be deleted by non-master token with right privilege
        $clientWithoutAdminToken->deleteTrigger((int) $trigger['id']);
        $triggerList = $clientWithoutAdminToken->listTriggers();
        $this->assertNotContainsTriggerId($trigger['id'], $triggerList);
        $this->assertCount(0, $triggerList);
    }

    /**
     * @param int $triggerId
     * @param array $triggerList
     * @return void
     */
    private function assertNotContainsTriggerId($triggerId, $triggerList)
    {
        $this->assertFalse(in_array($triggerId, array_map(function ($trigger) {
            return $trigger['id'];
        }, $triggerList)));
    }

    /**
     * @dataProvider deleteKeyProvider
     */
    public function testMissingParameters(string $keyToDelete, string $expectedMessage): void
    {
        $data = [
            'component' => 'orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => 20,
            'tableIds' => ['nothing-is-here'],
        ];
        unset($data[$keyToDelete]);
        $this->expectExceptionMessage($expectedMessage);
        $this->expectException(ClientException::class);
        $this->_client->createTrigger($data);
    }

    /**
     * @return void
     */
    public function testListAction(): void
    {
        $table = $this->createTableWithRandomData('watched-2');

        $options = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ);

        $newToken = $this->tokens->createToken($options);

        $trigger1ConfigurationId = time();
        $componentName = uniqid('test', true);
        $trigger1 = $this->_client->createTrigger([
            'component' => $componentName,
            'configurationId' => $trigger1ConfigurationId,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $newToken['id'],
            'tableIds' => [
                $table,
            ],
        ]);
        $trigger2 = $this->_client->createTrigger([
            'component' => 'keboola.ex-manzelka',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $newToken['id'],
            'tableIds' => [
                $table,
            ],
        ]);

        $triggers = $this->_client->listTriggers();
        $trigger1Found = $trigger2Found = false;
        foreach ($triggers as $trigger) {
            if ($trigger1['id'] === $trigger['id']) {
                $trigger1Found = true;
            }
            if ($trigger2['id'] === $trigger['id']) {
                $trigger2Found = true;
            }
        }

        $this->assertTrue($trigger1Found);
        $this->assertTrue($trigger2Found);

        $triggers = $this->_client->listTriggers(
            [
                'component' => $componentName,
                'configurationId' => $trigger1ConfigurationId,
            ],
        );

        $this->assertCount(1, $triggers);
        $this->assertEquals($trigger1['id'], $triggers[0]['id']);
    }

    /**
     * @return void
     */
    public function testInvalidToken(): void
    {
        $token = $this->tokens->createToken(new TokenCreateOptions());
        $this->tokens->dropToken($token['id']);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage("Token with id \"{$token['id']}\" was not found.");
        $this->_client->createTrigger([
            'component' => 'keboola.ex-manzelka',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $token['id'],
            'tableIds' => ['foo'],
        ]);
    }

    /**
     * @return void
     */
    public function testPreventTokenDelete(): void
    {
        $table1 = $this->createTableWithRandomData('watched-1');
        $options = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ);
        $newToken = $this->tokens->createToken($options);
        $trigger = $this->_client->createTrigger([
            'component' => 'orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $newToken['id'],
            'tableIds' => [
                $table1,
            ],
        ]);
        try {
            $this->tokens->dropToken($newToken['id']);
            $this->fail('Token should not be deleted');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.tokens.cannotDeleteDueToOrchestration', $e->getStringCode());
            $this->assertEquals(
                'Cannot delete token, because it\'s used for event trigger inside component "orchestrator" with configuration id "123"',
                $e->getMessage(),
            );
        }
        $this->_client->deleteTrigger($trigger['id']);
        $this->tokens->dropToken($newToken['id']);
    }

    /**
     * @return void
     */
    public function testTokenWithExpiration(): void
    {
        $token = $this->tokens->createToken(
            (new TokenCreateOptions())->setExpiresIn(5),
        );

        $this->expectExceptionCode(400);
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage("The 'runByToken' has expiration set. Use token without expiration.");
        $this->_client->createTrigger([
            'component' => 'keboola.ex-manzelka',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $token['id'],
            'tableIds' => ['foo'],
        ]);
    }

    /**
     * @return void
     */
    public function testTriggersRestrictionsForReadOnlyUser(): void
    {
        $expectedError = 'You don\'t have access to the resource.';
        $readOnlyClient = $this->getClientForToken(STORAGE_API_READ_ONLY_TOKEN);

        $table1 = $this->createTableWithRandomData('watched-1');
        $newToken = $this->tokens->createToken(
            (new TokenCreateOptions())
                ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ),
        );

        $options = [
            'component' => 'orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $newToken['id'],
            'tableIds' => [
                $table1,
            ],
        ];

        $trigger = $this->_client->createTrigger($options);
        $triggers = $this->_client->listTriggers();

        $this->assertSame($triggers, $readOnlyClient->listTriggers());

        try {
            $readOnlyClient->createTrigger($options);
            $this->fail('Triggers API POST request should be restricted for readOnly user');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('accessDenied', $e->getStringCode());
            $this->assertSame($expectedError, $e->getMessage());
        }

        try {
            $readOnlyClient->updateTrigger($trigger['id'], ['configurationId' => 987]);
            $this->fail('Triggers API PUT request should be restricted for readOnly user');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('accessDenied', $e->getStringCode());
            $this->assertSame($expectedError, $e->getMessage());
        }

        try {
            $readOnlyClient->deleteTrigger($trigger['id']);
            $this->fail('Triggers request should be restricted for readOnly user');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('accessDenied', $e->getStringCode());
            $this->assertSame($expectedError, $e->getMessage());
        }

        $this->assertSame($triggers, $this->_client->listTriggers());
    }

    /**
     * @return array
     */
    public function tokenCreateOptionsProvider()
    {
        // run setup manually to create bucket IDs
        $this->setUp();
        return [
            'can manage buckets only' => [
                (new TokenCreateOptions())
                    ->setCanManageBuckets(true),
            ],
            'component access only' => [
                (new TokenCreateOptions())
                    ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ)
                    ->addComponentAccess('keboola.orchestrator')
                    ->addComponentAccess('keboola.ex-1')
                    ->addComponentAccess('keboola.ex-2'),
            ],
            'both' => [
                (new TokenCreateOptions())
                    ->setCanManageBuckets(true)
                    ->addComponentAccess('keboola.orchestrator'),
            ],
            'access to wrong component but canManageBuckets' => [
                (new TokenCreateOptions())
                    ->setCanManageBuckets(true)
                    ->addComponentAccess('keboola.invalid'),
            ],
        ];
    }

    /**
     * @return array
     */
    public function tokenOptionsProviderInvalid()
    {
        $this->setUp();
        return [
            'component access only but cannot read bucket' => [
                (new TokenCreateOptions())
                    ->addComponentAccess('keboola.orchestrator'),
                "You don't have access to the resource.",
            ],
            'access to wrong component only' => [
                (new TokenCreateOptions())
                    ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ)
                    ->addComponentAccess('keboola.invalid'),
                'Your token does not have sufficient privilege. Token is neither admin token nor does it have canManageBucket permission nor does it have access to component(s) "keboola.orchestrator".',
            ],
            'no extra permissions, but not master token' => [
                (new TokenCreateOptions()),
                'Your token does not have sufficient privilege. Token is neither admin token nor does it have canManageBucket permission nor does it have access to component(s) "keboola.orchestrator".',
            ],
            'access to different component' => [
                (new TokenCreateOptions())
                    ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ)
                    ->addComponentAccess('keboola.wr-db'),
                'Your token does not have sufficient privilege. Token is neither admin token nor does it have canManageBucket permission nor does it have access to component(s) "keboola.orchestrator".',
            ],
        ];
    }

    /**
     * @return array
     */
    public function deleteKeyProvider()
    {
        return [
            'component' => [
                'component',
                'Invalid parameters - component: This field is missing.',
            ],
            'configurationId' => [
                'configurationId',
                'Invalid parameters - configurationId: This field is missing.',
            ],
            'coolDownPeriodMinutes' => [
                'coolDownPeriodMinutes',
                'Minimal cool down period is 1 minute',
            ],
            'runWithTokenId' => [
                'runWithTokenId',
                'Invalid parameters - runWithTokenId: This field is missing.',
            ],
            'tableIds' => [
                'tableIds',
                'Invalid parameters - tableIds: This field is missing.',
            ],
        ];
    }
}
