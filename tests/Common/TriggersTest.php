<?php



namespace Keboola\Test\Common;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\TokenAbstractOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\Test\StorageApiTestCase;

class TriggersTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();

        $this->_initEmptyTestBuckets();

        $triggers = $this->_client->listTriggers();
        foreach ($triggers as $trigger) {
            $this->_client->deleteTrigger((int) $trigger['id']);
        }
    }

    public function testCreateAndUpdateTrigger()
    {
        $table1 = $this->createTableWithRandomData("watched-1");
        $table2 = $this->createTableWithRandomData("watched-2");

        $options = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ)
        ;

        $newTokenId = $this->_client->createToken($options);

        $trigger = $this->_client->createTrigger([
            'component' => 'orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $newTokenId,
            'tableIds' => [
                $table1,
                $table2,
            ],
        ]);

        $this->assertEquals('orchestrator', $trigger['component']);
        $this->assertEquals(123, $trigger['configurationId']);
        $this->assertEquals(10, $trigger['coolDownPeriodMinutes']);
        $this->assertEquals($newTokenId, $trigger['runWithTokenId']);
        $this->assertEquals(null, $trigger['lastRun']);
        $this->assertEquals(
            [
                ['tableId' => 'in.c-API-tests.watched-1'],
                ['tableId' => 'in.c-API-tests.watched-2'],
            ],
            $trigger['tables']
        );

        $options = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ)
        ;

        $brandNewTokenId = $this->_client->createToken($options);

        $updateData = [
            'component' => 'keboola.ex-1',
            'configurationId' => 111,
            'coolDownPeriodMinutes' => 20,
            'runWithTokenId' => $brandNewTokenId,
            'tableIds' => [$table1],
        ];

        $updateTrigger = $this->_client->updateTrigger((int) $trigger['id'], $updateData);

        $this->assertEquals('keboola.ex-1', $updateTrigger['component']);
        $this->assertEquals(111, $updateTrigger['configurationId']);
        $this->assertEquals(20, $updateTrigger['coolDownPeriodMinutes']);
        $this->assertEquals($brandNewTokenId, $updateTrigger['runWithTokenId']);
        $this->assertEquals([['tableId' => 'in.c-API-tests.watched-1']], $updateTrigger['tables']);
    }

    /**
     * @dataProvider deleteKeyProvider
     */
    public function testMissingParameters($keyToDelete)
    {
        $data = [
            'component' => 'orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => 'nothing-is-here',
            'tableIds' => ['nothing-is-here'],
        ];
        unset($data[$keyToDelete]);
        $this->expectExceptionMessage(sprintf('Missing required query parameter(s) "%s"', $keyToDelete));
        $this->expectException(ClientException::class);
        $this->_client->createTrigger($data);
    }

    public function deleteKeyProvider()
    {
        return [
            ['component'],
            ['configurationId'],
            ['coolDownPeriodMinutes'],
            ['runWithTokenId'],
            ['tableIds'],
        ];
    }

    public function testDeleteTrigger()
    {
        $table = $this->createTableWithRandomData("watched-2");

        $options = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ)
        ;

        $newTokenId = $this->_client->createToken($options);

        $trigger = $this->_client->createTrigger([
            'component' => 'orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $newTokenId,
            'tableIds' => [
                $table,
            ],
        ]);

        $loadedTrigger = $this->_client->getTrigger((int) $trigger['id']);
        $this->assertEquals($trigger['id'], $loadedTrigger['id']);

        $this->_client->deleteTrigger((int) $loadedTrigger['id']);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(sprintf('Trigger with id [%d] was not found', $loadedTrigger['id']));
        $this->_client->getTrigger((int) $trigger['id']);
    }

    public function testUpdateTwoTables()
    {
        $table1 = $this->createTableWithRandomData("watched-1");
        $table2 = $this->createTableWithRandomData("watched-2");

        $options = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ)
        ;
        $newTokenId = $this->_client->createToken($options);

        $trigger1ConfigurationId = time();
        $componentName = uniqid('test');
        $trigger1Config = [
            'component' => $componentName,
            'configurationId' => $trigger1ConfigurationId,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $newTokenId,
            'tableIds' => [
                $table1,
            ],
        ];
        $trigger1 = $this->_client->createTrigger($trigger1Config);
        $trigger2Config = [
            'component' => 'keboola.ex-manzelka',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $newTokenId,
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
            if ($trigger1['id'] == $trigger['id']) {
                $trigger1Found = true;
                $this->assertSame($trigger1Config['tableIds'][0], $trigger['tables'][0]['tableId']);
            }
            if ($trigger2['id'] == $trigger['id']) {
                $trigger2Found = true;
                $this->assertSame($trigger2Config['tableIds'][0], $trigger['tables'][0]['tableId']);
            }
        }

        $this->assertTrue($trigger1Found);
        $this->assertTrue($trigger2Found);
    }

    public function testListAction()
    {
        $table = $this->createTableWithRandomData("watched-2");

        $options = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ)
        ;

        $newTokenId = $this->_client->createToken($options);

        $trigger1ConfigurationId = time();
        $componentName = uniqid('test');
        $trigger1 = $this->_client->createTrigger([
            'component' => $componentName,
            'configurationId' => $trigger1ConfigurationId,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $newTokenId,
            'tableIds' => [
                $table,
            ],
        ]);
        $trigger2 = $this->_client->createTrigger([
            'component' => 'keboola.ex-manzelka',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $newTokenId,
            'tableIds' => [
                $table,
            ],
        ]);

        $triggers = $this->_client->listTriggers();
        $trigger1Found = $trigger2Found = false;
        foreach ($triggers as $trigger) {
            if ($trigger1['id'] == $trigger['id']) {
                $trigger1Found = true;
            }
            if ($trigger2['id'] == $trigger['id']) {
                $trigger2Found = true;
            }
        }

        $this->assertTrue($trigger1Found);
        $this->assertTrue($trigger2Found);

        $triggers = $this->_client->listTriggers(
            [
                'component' => $componentName,
                'configurationId' => $trigger1ConfigurationId
            ]
        );

        $this->assertCount(1, $triggers);
        $this->assertEquals($trigger1['id'], $triggers[0]['id']);
    }

    public function testPreventTokenDelete()
    {
        $table1 = $this->createTableWithRandomData("watched-1");

        $options = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ)
        ;

        $newTokenId = $this->_client->createToken($options);

        $trigger = $this->_client->createTrigger([
            'component' => 'orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $newTokenId,
            'tableIds' => [
                $table1,
            ],
        ]);

        try {
            $this->_client->dropToken($newTokenId);
            $this->fail("Token should not be deleted");
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
            $this->assertEquals('storage.tokens.cannotDeleteDueToOrchestration', $e->getStringCode());
            $this->assertEquals(
                'Cannot delete token, bacause it\'s used for event trigger inside component "orchestrator" with configuration id "123"',
                $e->getMessage()
            );
        }

        $this->_client->deleteTrigger($trigger['id']);
        $this->_client->dropToken($newTokenId);
    }
}
