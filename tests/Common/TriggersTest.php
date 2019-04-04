<?php

declare(strict_types=1);

namespace Keboola\Test\Common;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;

class TriggersTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testCreateAndUpdateTrigger(): void
    {
        $table1 = $this->createTableWithRandomData("watched-1");
        $table2 = $this->createTableWithRandomData("watched-2");
        $newTokenId = $this->_client->createToken([$this->getTestBucketId() => 'read']);

        $trigger = $this->_client->createTrigger([
            'component' => 'orchestrator',
            'configurationId' => 123,
            'coolDownPeriod' => 10,
            'runWithTokenId' => $newTokenId,
            'tableIds' => [
                $table1,
                $table2,
            ],
        ]);

        $this->assertEquals('orchestrator', $trigger['component']);
        $this->assertEquals(123, $trigger['configurationId']);
        $this->assertEquals(10, $trigger['coolDownPeriod']);
        $this->assertEquals($newTokenId, $trigger['runWithTokenId']);
        $this->assertEquals(null, $trigger['lastRun']);
        $this->assertEquals(
            [
                ['tableId' => 'in.c-API-tests.watched-1'],
                ['tableId' => 'in.c-API-tests.watched-2'],
            ],
            $trigger['tables']
        );

        $brandNewTokenId = $this->_client->createToken([$this->getTestBucketId() => 'read']);

        $updateData = [
            'component' => 'keboola.ex-1',
            'configurationId' => 111,
            'coolDownPeriod' => 20,
            'runWithTokenId' => $brandNewTokenId,
            'tableIds' => [$table1],
        ];

        $updateTrigger = $this->_client->updateTrigger($trigger['id'], $updateData);

        $this->assertEquals('keboola.ex-1', $updateTrigger['component']);
        $this->assertEquals(111, $updateTrigger['configurationId']);
        $this->assertEquals(20, $updateTrigger['coolDownPeriod']);
        $this->assertEquals($brandNewTokenId, $updateTrigger['runWithTokenId']);
        $this->assertEquals([['tableId' => 'in.c-API-tests.watched-1']], $updateTrigger['tables']);
    }

    /**
     * @dataProvider deleteKeyProvider
     */
    public function testMissingParameters($keyToDelete): void
    {
        $data = [
            'component' => 'orchestrator',
            'configurationId' => 123,
            'coolDownPeriod' => 10,
            'runWithTokenId' => 'nothing-is-here',
            'tableIds' => ['nothing-is-here'],
        ];
        unset($data[$keyToDelete]);
        $this->expectExceptionMessage(sprintf('Missing required query parametr(s) [%s]', $keyToDelete));
        $this->expectException(ClientException::class);
        $this->_client->createTrigger($data);
    }

    public function deleteKeyProvider(): array
    {
        return [
            ['component'],
            ['configurationId'],
            ['coolDownPeriod'],
            ['runWithTokenId'],
            ['tableIds'],
        ];
    }

    public function testDeleteTrigger(): void
    {
        $table = $this->createTableWithRandomData("watched-2");
        $newTokenId = $this->_client->createToken([$this->getTestBucketId() => 'read']);

        $trigger = $this->_client->createTrigger([
            'component' => 'orchestrator',
            'configurationId' => 123,
            'coolDownPeriod' => 10,
            'runWithTokenId' => $newTokenId,
            'tableIds' => [
                $table,
            ],
        ]);

        $loadedTrigger = $this->_client->getTrigger($trigger['id']);
        $this->assertEquals($trigger['id'], $loadedTrigger['id']);

        $this->_client->deleteTrigger($loadedTrigger['id']);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(sprintf('Trigger with id [%d] was not found', $loadedTrigger['id']));
        $this->_client->getTrigger($trigger['id']);
    }
}
